#!/usr/bin/env python3
"""Synthetic data generator for the ride-hailing operational database.

The script resets a subset of the OLTP schema and reseeds it with internally
consistent booking, ride and financial data so the CDC → Flink → Mart pipeline
can be exercised end-to-end.  It is intentionally opinionated but resilient to
schema drift: before inserting any payload, we introspect the live Postgres
catalog and only include columns that actually exist.  This avoids hard
failures such as `column "id_hashed" does not exist` when different bootstrap
scripts are used in development.

Key design goals
----------------
* Allow different "volume" presets (tiny/small/medium/large) so developers can
  smoke-test quickly or stress the pipeline with a heavier load.
* Keep business metrics self-consistent.  Every booking has a deterministic
  status transition timeline and money flow that reconcile to the derived KPI
  definitions (GSV, revenue, net driver income, discount, etc.).
* Populate auxiliary tables (accounts, driver earnings, passenger addresses,
  cancellations, driver availability logs) often required by dashboards or
  downstream enrichment.
* Be idempotent across reruns via full truncate + insert with stable UUIDs per
  run.

Usage examples
--------------

```bash
# Default: connect to localhost:5432/ride_hailing_db (user/password) and seed
# the "small" volume profile (~1k bookings)
python3 generator/data_generator.py

# Seed a larger dataset into a custom schema with environment overrides
PGHOST=postgres-source PGPORT=5432 PGUSER=user PGPASSWORD=password \
  python3 generator/data_generator.py --volume medium --pg-schema public

# Skip truncate when you only want to append more synthetic data
python3 generator/data_generator.py --skip-reset
```

The generator requires `psycopg2-binary` and `faker`; install them with
`pip install psycopg2-binary Faker` if they are not already available.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import random
import sys
import textwrap
import time
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Iterable, List, Mapping, MutableMapping, Optional, Sequence, Tuple

import psycopg2
import psycopg2.extras


# Register uuid adapter early so both psycopg2-binary and the system psycopg2
# packages can serialise ``uuid.UUID`` values even when the underlying column is
# declared as ``uuid``.  Some environments (notably macOS system Python) do not
# enable the adapter by default which leads to "can't adapt type 'UUID'"
# errors when batching inserts.
psycopg2.extras.register_uuid()
from faker import Faker


# --------------------------------------------------------------------------------------
# Configuration & presets
# --------------------------------------------------------------------------------------

DEFAULT_DB = {
    "host": os.getenv("PGHOST", "localhost"),
    "port": int(os.getenv("PGPORT", "5432")),
    "dbname": os.getenv("PGDATABASE", "ride_hailing_db"),
    "user": os.getenv("PGUSER", "user"),
    "password": os.getenv("PGPASSWORD", "password"),
}

VOLUME_PRESETS = {
    "tiny":   {"rides": 200,   "drivers": 120,  "passengers": 240},
    "small":  {"rides": 1000,  "drivers": 500,  "passengers": 1000},
    "medium": {"rides": 5000,  "drivers": 2000, "passengers": 4000},
    "large":  {"rides": 15000, "drivers": 6000, "passengers": 12000},
}

# Streaming mode presets scale the live event rate based on the requested
# volume profile.  Users can still override any parameter explicitly via the
# CLI; the preset only applies when a flag is left unspecified.
STREAM_PRESETS = {
    "tiny": {
        "stream_drivers": 120,
        "stream_passengers": 240,
        "stream_max_inflight": 120,
        "stream_new_booking_prob": 0.35,
        "stream_driver_updates": 3,
        "stream_cancel_prob": 0.10,
        "stream_sleep_min": 0.6,
        "stream_sleep_max": 1.4,
        "stream_step_min": 5,
        "stream_step_max": 20,
        "stream_log_interval": 40,
    },
    "small": {
        "stream_drivers": 400,
        "stream_passengers": 800,
        "stream_max_inflight": 600,
        "stream_new_booking_prob": 0.65,
        "stream_driver_updates": 8,
        "stream_cancel_prob": 0.12,
        "stream_sleep_min": 0.35,
        "stream_sleep_max": 0.9,
        "stream_step_min": 4,
        "stream_step_max": 18,
        "stream_log_interval": 30,
    },
    "medium": {
        "stream_drivers": 1200,
        "stream_passengers": 2400,
        "stream_max_inflight": 2500,
        "stream_new_booking_prob": 0.85,
        "stream_driver_updates": 15,
        "stream_cancel_prob": 0.14,
        "stream_sleep_min": 0.18,
        "stream_sleep_max": 0.45,
        "stream_step_min": 3,
        "stream_step_max": 14,
        "stream_log_interval": 20,
    },
    "large": {
        "stream_drivers": 3000,
        "stream_passengers": 6000,
        "stream_max_inflight": 6000,
        "stream_new_booking_prob": 0.97,
        "stream_driver_updates": 28,
        "stream_cancel_prob": 0.15,
        "stream_sleep_min": 0.05,
        "stream_sleep_max": 0.15,
        "stream_step_min": 2,
        "stream_step_max": 10,
        "stream_log_interval": 15,
    },
}

SERVICE_TYPES = ["BIKE", "CAR"]
SERVICE_TIERS = ["ECONOMY", "PREMIUM"]
AREAS = ["Q1", "Q3", "Q7", "BT", "TP", "DN", "HP", "BD"]
COUNTRY_CODES = ["VN", "SG"]

VAT_RATE = Decimal("0.10")  # 10% VAT (khớp với Flink job)

faker = Faker("vi_VN")
Faker.seed(2024)
random.seed(2024)


# --------------------------------------------------------------------------------------
# Helper utilities
# --------------------------------------------------------------------------------------


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def minutes_ago(minutes: int) -> datetime:
    return utc_now() - timedelta(minutes=minutes)


def rand_point() -> Tuple[float, float]:
    """Return a pseudo-random coordinate around HCMC."""

    lon = random.uniform(106.60, 106.90)
    lat = random.uniform(10.70, 11.00)
    return lon, lat


def quantize_money(value: float) -> int:
    return int(Decimal(value).quantize(Decimal("1")))


def to_uuid(value: Optional[str] = None) -> uuid.UUID:
    return uuid.UUID(str(value)) if value else uuid.uuid4()


def chunked(seq: Sequence, size: int) -> Iterable[Sequence]:
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def pg_ident(ident: str) -> str:
    return '"' + ident.replace('"', '""') + '"'


def print_header(volume_name: str, preset: Mapping[str, int], db_cfg: Mapping[str, object], schema: str) -> None:
    banner = textwrap.dedent(
        f"""
        ======================================================================
        🚀 MOOVTEK DATA GENERATOR → Postgres (consistent money/keys)
        ======================================================================
          Volume:       {volume_name:<6} (rides={preset['rides']:,}, drivers={preset['drivers']:,}, passengers={preset['passengers']:,})
          PG Target:    {db_cfg['user']}@{db_cfg['host']}:{db_cfg['port']}/{db_cfg['dbname']}  schema={schema}
        ======================================================================
        """
    ).strip("\n")
    print(banner)
    sys.stdout.flush()


# --------------------------------------------------------------------------------------
# Database helpers
# --------------------------------------------------------------------------------------


def connect(db_cfg: Mapping[str, object]) -> psycopg2.extensions.connection:
    while True:
        try:
            conn = psycopg2.connect(**db_cfg)
            conn.autocommit = True
            return conn
        except psycopg2.OperationalError as exc:  # pragma: no cover - connection retry
            print(f"[wait] PostgreSQL not ready: {exc}")
            time.sleep(2)


def fetch_table_columns(conn, schema: str) -> Mapping[str, List[str]]:
    sql = textwrap.dedent(
        """
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = %s
        ORDER BY table_name, ordinal_position
        """
    )
    cur = conn.cursor()
    cur.execute(sql, (schema,))
    mapping: MutableMapping[str, List[str]] = defaultdict(list)
    for table, column in cur.fetchall():
        mapping[table].append(column)
    return mapping


def fetch_existing_tables(conn, schema: str) -> Mapping[str, str]:
    sql = textwrap.dedent(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s AND table_type = 'BASE TABLE'
        """
    )
    cur = conn.cursor()
    cur.execute(sql, (schema,))
    return {row[0]: row[0] for row in cur.fetchall()}


def truncate_tables(conn, schema: str, tables: Iterable[str]) -> None:
    cur = conn.cursor()
    for table in tables:
        cur.execute(
            f"TRUNCATE TABLE {pg_ident(schema)}.{pg_ident(table)} RESTART IDENTITY CASCADE"
        )


def batch_upsert(
    conn,
    schema: str,
    table: str,
    rows: Sequence[Mapping[str, object]],
    pk: str = "id",
    do_update: bool = False,
    column_cache: Optional[Mapping[str, Sequence[str]]] = None,
) -> int:
    if not rows:
        return 0

    if column_cache is None:
        column_cache = fetch_table_columns(conn, schema)

    table_columns = set(column_cache.get(table, []))
    if not table_columns:
        return 0

    filtered: List[Mapping[str, object]] = []
    for row in rows:
        usable = {k: v for k, v in row.items() if k in table_columns}
        if pk not in usable:
            raise ValueError(f"Primary key '{pk}' missing for table {table}")
        filtered.append(usable)

    all_columns: List[str] = sorted({k for row in filtered for k in row})
    column_sql = ", ".join(pg_ident(col) for col in all_columns)
    placeholder = ", ".join(["%s"] * len(all_columns))

    if do_update and len(all_columns) > 1:
        updates = ", ".join(
            f"{pg_ident(col)} = EXCLUDED.{pg_ident(col)}"
            for col in all_columns
            if col != pk
        )
        conflict_sql = f"ON CONFLICT ({pg_ident(pk)}) DO UPDATE SET {updates}"
    else:
        conflict_sql = f"ON CONFLICT ({pg_ident(pk)}) DO NOTHING"

    sql = (
        f"INSERT INTO {pg_ident(schema)}.{pg_ident(table)} ({column_sql}) "
        f"VALUES ({placeholder}) {conflict_sql}"
    )

    def _normalise(value: object) -> object:
        if isinstance(value, uuid.UUID):
            return str(value)
        return value

    payload = [tuple(_normalise(row.get(col)) for col in all_columns) for row in filtered]
    psycopg2.extras.execute_batch(conn.cursor(), sql, payload, page_size=1000)
    return len(payload)


# --------------------------------------------------------------------------------------
# Synthetic data generation
# --------------------------------------------------------------------------------------


def _random_email(prefix: str) -> str:
    slug = "".join(ch for ch in prefix.lower() if ch.isalnum()) or "user"
    return f"{slug[:10]}-{uuid.uuid4().hex[:10]}@example.com"


def build_driver(driver_id: uuid.UUID, now: datetime) -> Mapping[str, object]:
    full_name = faker.name()
    return {
        "id": driver_id,
        "fullName": full_name,
        "email": _random_email(full_name),
        "phoneNumber": random.randint(100_000_000, 999_999_999),
        "countryCode": random.choice(COUNTRY_CODES),
        "status": "ACTIVE",
        "password": hashlib.sha256(full_name.encode("utf-8")).hexdigest(),
        "areaCode": random.choice(AREAS),
        "serviceCode": random.choice(SERVICE_TYPES),
        "currentStepRegistration": "COMPLETED",
        "verificationStatus": "VERIFIED",
        "isMoovTek": False,
        "isMoovTekPlatform": True,
        "createdAt": now,
        "updatedAt": now,
    }


def build_passenger(passenger_id: uuid.UUID, now: datetime) -> Mapping[str, object]:
    full_name = faker.name()
    return {
        "id": passenger_id,
        "fullName": full_name,
        "email": _random_email(full_name),
        "phoneNumber": random.randint(1_000_000_000, 9_999_999_999),
        "countryCode": random.choice(COUNTRY_CODES),
        "status": "ACTIVE",
        "password": hashlib.sha1(passenger_id.bytes).hexdigest(),
        "createdAt": now,
        "updatedAt": now,
    }


def generate_entities(volume: Mapping[str, int]) -> Tuple[List[Mapping[str, object]], List[Mapping[str, object]]]:
    now = utc_now()
    drivers = [build_driver(uuid.uuid4(), now) for _ in range(volume["drivers"])]
    passengers = [build_passenger(uuid.uuid4(), now) for _ in range(volume["passengers"])]
    return drivers, passengers


def generate_driver_locations(drivers: Sequence[Mapping[str, object]]) -> List[Mapping[str, object]]:
    items: List[Mapping[str, object]] = []
    for driver in drivers:
        lon, lat = rand_point()
        items.append(
            {
                "id": uuid.uuid4(),
                "driverId": driver["id"],
                "coordinates": f"SRID=4326;POINT({lon:.6f} {lat:.6f})",
                "availability": random.choice(["AVAILABLE", "BUSY", "UNAVAILABLE"]),
                "createdAt": minutes_ago(random.randint(30, 720)),
                "updatedAt": minutes_ago(random.randint(0, 15)),
            }
        )
    return items


def generate_driver_availability(drivers: Sequence[Mapping[str, object]]) -> List[Mapping[str, object]]:
    logs: List[Mapping[str, object]] = []
    for driver in drivers:
        entries = random.randint(3, 8)
        base = minutes_ago(random.randint(60, 720))
        state = "OFFLINE"
        for i in range(entries):
            state = random.choice(["AVAILABLE", "BUSY", "UNAVAILABLE"])
            logs.append(
                {
                    "id": uuid.uuid4(),
                    "driverId": driver["id"],
                    "availability": state,
                    "createdAt": base + timedelta(minutes=i * random.randint(5, 20)),
                    "updatedAt": base + timedelta(minutes=i * random.randint(5, 20)),
                }
            )
    return logs


def generate_rides_and_bookings(
    volume: Mapping[str, int],
    drivers: Sequence[Mapping[str, object]],
    passengers: Sequence[Mapping[str, object]],
) -> Tuple[List[Mapping[str, object]], List[Mapping[str, object]], List[Mapping[str, object]], List[Mapping[str, object]], List[Mapping[str, object]]]:
    rides: List[Mapping[str, object]] = []
    bookings: List[Mapping[str, object]] = []
    driver_trips: List[Mapping[str, object]] = []
    cancellations: List[Mapping[str, object]] = []
    driver_earnings: List[Mapping[str, object]] = []

    driver_cycle = iter(drivers)
    passenger_cycle = iter(passengers)

    for i in range(volume["rides"]):
        try:
            driver = next(driver_cycle)
        except StopIteration:
            driver_cycle = iter(drivers)
            driver = next(driver_cycle)

        try:
            passenger = next(passenger_cycle)
        except StopIteration:
            passenger_cycle = iter(passengers)
            passenger = next(passenger_cycle)

        base_time = minutes_ago(random.randint(30, 24 * 60))
        service_type = driver.get("serviceCode") or random.choice(SERVICE_TYPES)
        service_tier = random.choice(SERVICE_TIERS)
        area = driver.get("areaCode") or random.choice(AREAS)
        lon1, lat1 = rand_point()
        lon2, lat2 = rand_point()

        ride_id = uuid.uuid4()
        booking_id = uuid.uuid4()
        status_flow = ["REQUESTED", "DRIVER_ACCEPTED", "AT_PICKUP", "STARTED", "COMPLETED"]
        is_cancelled = random.random() < 0.12

        created_at = base_time
        accepted_at = created_at + timedelta(minutes=random.randint(1, 5))
        pickup_at = accepted_at + timedelta(minutes=random.randint(1, 4))
        start_at = pickup_at + timedelta(minutes=random.randint(1, 6))
        completed_at = start_at + timedelta(minutes=random.randint(5, 30))

        total_amount = quantize_money(random.uniform(50_000, 250_000))
        discount = quantize_money(total_amount * random.uniform(0, 0.2))
        platform_fee = quantize_money((total_amount - discount) * Decimal("0.20"))
        total_after_discount = total_amount - discount
        additional = quantize_money(random.uniform(0, 15_000))
        total_before_discount = total_after_discount + discount
        driver_income = total_after_discount - platform_fee + additional

        ride_status = "CANCELED" if is_cancelled else "COMPLETED"
        ride_cancel_at = accepted_at + timedelta(minutes=random.randint(1, 3)) if is_cancelled else None

        rides.append(
            {
                "id": ride_id,
                "driverId": driver["id"],
                "startAddress": faker.street_address(),
                "startLocation": f"SRID=4326;POINT({lon1:.6f} {lat1:.6f})",
                "endAddress": faker.street_address(),
                "endLocation": f"SRID=4326;POINT({lon2:.6f} {lat2:.6f})",
                "status": ride_status,
                "seats": 1,
                "serviceTypeCode": service_type,
                "serviceTierCode": service_tier,
                "cancelAt": ride_cancel_at,
                "createdAt": created_at,
                "updatedAt": completed_at if not is_cancelled else ride_cancel_at,
                "serviceVariantsCode": service_tier,
                "isMoovTekPool": False,
                "totalAmount": total_amount,
            }
        )

        booking_status = "CANCELED" if is_cancelled else "COMPLETED"
        bookings.append(
            {
                "id": booking_id,
                "rideId": ride_id,
                "bookingCode": f"BK-{i:06d}",
                "passengerId": passenger["id"],
                "pickupAddress": faker.street_address(),
                "pickupLocation": f"SRID=4326;POINT({lon1:.6f} {lat1:.6f})",
                "dropoffAddress": faker.street_address(),
                "dropoffLocation": f"SRID=4326;POINT({lon2:.6f} {lat2:.6f})",
                "serviceTypeCode": service_type,
                "serviceTierCode": service_tier,
                "discountAmount": discount,
                "platformFee": platform_fee,
                "totalAmountBeforeDiscount": total_before_discount,
                "totalAmountAfterDiscount": total_after_discount,
                "additionalCharges": additional,
                "totalAmount": total_after_discount + additional,
                "status": booking_status,
                "startTripAt": start_at if not is_cancelled else None,
                "atPickUpAt": pickup_at if not is_cancelled else None,
                "completedAt": completed_at if not is_cancelled else None,
                "createdAt": created_at,
                "updatedAt": completed_at if not is_cancelled else ride_cancel_at,
                "areaCode": area,
                "wardCode": area,
                "serviceVariantsCode": service_tier,
                "seats": 1,
            }
        )

        driver_trips.append(
            {
                "id": uuid.uuid4(),
                "driverId": driver["id"],
                "rideId": ride_id,
                "bookingId": booking_id,
                "status": "COMPLETED" if not is_cancelled else "CANCELED",
                "createdAt": created_at,
                "updatedAt": completed_at if not is_cancelled else ride_cancel_at,
                "assignedTripAt": accepted_at,
                "acceptedTripAt": accepted_at,
                "assignedLocation": f"SRID=4326;POINT({lon1:.6f} {lat1:.6f})",
                "acceptedLocation": f"SRID=4326;POINT({lon1:.6f} {lat1:.6f})",
                "includeInMetric": True,
            }
        )

        if is_cancelled:
            cancellations.append(
                {
                    "id": uuid.uuid4(),
                    "rideId": ride_id,
                    "bookingId": booking_id,
                    "reasonDetail": random.choice([
                        "Passenger no-show",
                        "Driver emergency",
                        "Payment issue",
                        "Weather alert",
                    ]),
                    "cancelledById": passenger["id"],
                    "cancelledBy": random.choice(["PASSENGER", "DRIVER"]),
                    "cancelledAt": ride_cancel_at,
                    "createdAt": ride_cancel_at,
                    "updatedAt": ride_cancel_at,
                    "status": "APPROVED",
                }
            )
        else:
            driver_earnings.append(
                {
                    "id": uuid.uuid4(),
                    "rideId": ride_id,
                    "bookingId": booking_id,
                    "driverId": driver["id"],
                    "serviceFeeBeforeTax": platform_fee,
                    "commissionFee": platform_fee,
                    "personalIncomeTax": quantize_money(platform_fee * VAT_RATE),
                    "taxAmount": quantize_money(platform_fee * VAT_RATE),
                    "otherFee": 0,
                    "driverEarning": driver_income,
                    "totalEarnings": driver_income,
                    "createdAt": completed_at,
                    "updatedAt": completed_at,
                    "bonusMoney": 0,
                    "bonusPITax": 0,
                    "bonusAfterPITax": 0,
                    "bonusRate": Decimal("0"),
                    "diamondReceived": 0,
                    "commissionRate": Decimal("0.20"),
                    "totalAmountBooking": total_after_discount,
                }
            )

    return rides, bookings, driver_trips, cancellations, driver_earnings


def generate_accounts(drivers: Sequence[Mapping[str, object]], driver_earnings: Sequence[Mapping[str, object]]) -> Tuple[List[Mapping[str, object]], List[Mapping[str, object]], List[Mapping[str, object]]]:
    accounts: List[Mapping[str, object]] = []
    histories: List[Mapping[str, object]] = []
    transactions: List[Mapping[str, object]] = []

    earning_by_driver: MutableMapping[uuid.UUID, int] = defaultdict(int)
    for earning in driver_earnings:
        earning_by_driver[earning["driverId"]] += int(earning["driverEarning"])

    for driver in drivers:
        account_id = uuid.uuid4()
        balance = earning_by_driver.get(driver["id"], 0)
        created_at = minutes_ago(random.randint(60, 24 * 60))

        accounts.append(
            {
                "id": account_id,
                "driverId": driver["id"],
                "accountType": "EARNINGS",
                "balance": Decimal(balance),
                "currency": "VND",
                "lastWithdrawalDate": created_at - timedelta(days=random.randint(1, 7)),
                "createdAt": created_at,
                "updatedAt": created_at,
                "isVerifiedQuickWithdraw": True,
            }
        )

        if balance <= 0:
            continue

        transaction_id = uuid.uuid4()
        transactions.append(
            {
                "id": transaction_id,
                "driverId": driver["id"],
                "accountId": account_id,
                "accountType": "EARNINGS",
                "transactionType": "PAYOUT",
                "transactionMethod": "BANK",
                "transactionRef": f"WD-{transaction_id.hex[:10]}",
                "status": "COMPLETED",
                "amount": Decimal(balance),
                "currency": "VND",
                "createdAt": created_at,
                "updatedAt": created_at,
                "bookingCode": None,
                "title": "Weekly payout",
                "isQuickWithdraw": False,
            }
        )

        histories.append(
            {
                "id": uuid.uuid4(),
                "accountId": account_id,
                "transactionId": transaction_id,
                "amount": Decimal(balance),
                "previousBalance": Decimal(0),
                "newBalance": Decimal(balance),
                "createdAt": created_at,
                "updatedAt": created_at,
            }
        )

    return accounts, histories, transactions


def generate_passenger_addresses(passengers: Sequence[Mapping[str, object]]) -> List[Mapping[str, object]]:
    records: List[Mapping[str, object]] = []
    for passenger in passengers:
        for addr_type in ["HOME", "WORK"]:
            if random.random() < 0.6:
                lon, lat = rand_point()
                records.append(
                    {
                        "id": uuid.uuid4(),
                        "passengerId": passenger["id"],
                        "addressType": addr_type,
                        "address": faker.street_address(),
                        "location": f"SRID=4326;POINT({lon:.6f} {lat:.6f})",
                        "isDefault": addr_type == "HOME",
                        "createdAt": minutes_ago(random.randint(60, 48 * 60)),
                        "updatedAt": minutes_ago(random.randint(0, 60)),
                    }
                )
    return records


# --------------------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------------------


def seed_postgres(
    conn,
    schema: str,
    drivers: Sequence[Mapping[str, object]],
    passengers: Sequence[Mapping[str, object]],
    rides: Sequence[Mapping[str, object]],
    bookings: Sequence[Mapping[str, object]],
    driver_trips: Sequence[Mapping[str, object]],
    cancellations: Sequence[Mapping[str, object]],
    driver_earnings: Sequence[Mapping[str, object]],
    driver_locations: Sequence[Mapping[str, object]],
    availability_logs: Sequence[Mapping[str, object]],
    accounts: Sequence[Mapping[str, object]],
    histories: Sequence[Mapping[str, object]],
    transactions: Sequence[Mapping[str, object]],
    passenger_addresses: Sequence[Mapping[str, object]],
    skip_reset: bool,
) -> None:
    tables_payload = {
        "driver": (drivers, "id"),
        "passenger": (passengers, "id"),
        "rides": (rides, "id"),
        "booking": (bookings, "id"),
        "driver_trip": (driver_trips, "id"),
        "booking_cancellations": (cancellations, "id"),
        "driver_earnings": (driver_earnings, "id"),
        "driver_location": (driver_locations, "id"),
        "driver_availability_logs": (availability_logs, "id"),
        "account": (accounts, "id"),
        "account_history": (histories, "id"),
        "account_transaction": (transactions, "id"),
        "passenger_address": (passenger_addresses, "id"),
    }

    existing_tables = fetch_existing_tables(conn, schema)
    filtered_tables = [tbl for tbl in tables_payload if tbl in existing_tables]

    if not skip_reset:
        print("\n🧹 Truncating tables ...")
        truncate_tables(conn, schema, filtered_tables)

    print("\n⬆️  Seeding into Postgres ...")
    total_inserted = 0
    column_cache = fetch_table_columns(conn, schema)
    for table_name in filtered_tables:
        rows, pk = tables_payload[table_name]
        if not rows:
            continue
        inserted = batch_upsert(
            conn,
            schema,
            table_name,
            rows,
            pk=pk,
            do_update=False,
            column_cache=column_cache,
        )
        total_inserted += inserted
        print(f"   • {table_name:<28} inserted {inserted:,} rows")

    print(f"\n✅ Done. Inserted {total_inserted:,} rows across {len(filtered_tables)} tables.")


# --------------------------------------------------------------------------------------
# Streaming mode helpers
# --------------------------------------------------------------------------------------


def ensure_min_entities(
    conn,
    schema: str,
    table: str,
    target_count: int,
    builder,
    column_cache: Optional[Mapping[str, Sequence[str]]] = None,
) -> List[uuid.UUID]:
    cur = conn.cursor()
    cur.execute(
        f"SELECT COUNT(*) FROM {pg_ident(schema)}.{pg_ident(table)}"
    )
    current = cur.fetchone()[0]
    needed = max(0, target_count - current)

    if needed > 0:
        now = utc_now()
        try:
            faker.unique.clear()
        except AttributeError:
            pass
        rows = [builder(uuid.uuid4(), now) for _ in range(needed)]
        batch_upsert(
            conn,
            schema,
            table,
            rows,
            pk="id",
            do_update=False,
            column_cache=column_cache,
        )

    cur.execute(
        f"SELECT id FROM {pg_ident(schema)}.{pg_ident(table)}"
    )
    return [row[0] for row in cur.fetchall()]


def ensure_driver_locations(
    conn,
    schema: str,
    driver_ids: Sequence[uuid.UUID],
    column_cache: Optional[Mapping[str, Sequence[str]]] = None,
) -> None:
    cur = conn.cursor()
    cur.execute(
        f"SELECT \"driverId\" FROM {pg_ident(schema)}.{pg_ident('driver_location')}"
    )
    existing = {row[0] for row in cur.fetchall()}
    missing = [driver_id for driver_id in driver_ids if driver_id not in existing]
    if not missing:
        return

    now = utc_now()
    rows: List[Mapping[str, object]] = []
    for driver_id in missing:
        lon, lat = rand_point()
        rows.append(
            {
                "id": uuid.uuid4(),
                "driverId": driver_id,
                "coordinates": f"SRID=4326;POINT({lon:.6f} {lat:.6f})",
                "availability": "AVAILABLE",
                "createdAt": now,
                "updatedAt": now,
            }
        )

    batch_upsert(
        conn,
        schema,
        "driver_location",
        rows,
        pk="id",
        do_update=False,
        column_cache=column_cache,
    )


def stream_create_booking(
    cur,
    schema: str,
    passenger_id: uuid.UUID,
    booking_columns: Sequence[str],
) -> Tuple[uuid.UUID, datetime]:
    lon1, lat1 = rand_point()
    lon2, lat2 = rand_point()
    booking_id = uuid.uuid4()
    now = utc_now()
    service_type = random.choice(SERVICE_TYPES)
    service_tier = random.choice(SERVICE_TIERS)
    ward = random.choice(AREAS)
    discount = random.randint(0, 20_000)
    platform_fee = random.randint(20_000, 120_000)
    total_before = platform_fee + random.randint(10_000, 80_000)
    total_after = max(total_before - discount, 0)

    values = {
        "id": booking_id,
        "passengerId": passenger_id,
        "status": "REQUESTED",
        "serviceTypeCode": service_type,
        "serviceTierCode": service_tier,
        "pickupAddress": "Pickup",
        "dropoffAddress": "Dropoff",
        "pickupLocation": f"SRID=4326;POINT({lon1} {lat1})",
        "dropoffLocation": f"SRID=4326;POINT({lon2} {lat2})",
        "wardCode": ward,
        "discountAmount": discount,
        "platformFee": platform_fee,
        "totalAmountBeforeDiscount": total_before,
        "totalAmountAfterDiscount": total_after,
        "totalAmount": total_after,
        "serviceVariantsCode": random.choice(SERVICE_TIERS),
        "createdAt": now,
        "updatedAt": now,
    }

    cols = [c for c in values if c in booking_columns]
    placeholders = ["%s"] * len(cols)
    sql = textwrap.dedent(
        f"""
        INSERT INTO {pg_ident(schema)}.{pg_ident('booking')} (
          {', '.join(pg_ident(c) for c in cols)}
        ) VALUES (
          {', '.join(placeholders)}
        )
        """
    )
    params = [values[c] for c in cols]
    cur.execute(sql, params)

    return booking_id, now


def stream_advance_booking(
    cur,
    schema: str,
    booking_id: uuid.UUID,
    current_status: str,
    current_ts: datetime,
    cancel_prob: float,
    step_min: int,
    step_max: int,
    booking_columns: Sequence[str],
) -> Tuple[str, datetime]:
    transitions = {
        "REQUESTED": "DRIVER_ACCEPTED",
        "DRIVER_ACCEPTED": "AT_PICKUP",
        "AT_PICKUP": "STARTED",
        "STARTED": "COMPLETED",
        "COMPLETED": "COMPLETED",
    }

    next_status = transitions.get(current_status, "COMPLETED")
    if current_status in {"REQUESTED", "DRIVER_ACCEPTED"} and random.random() < cancel_prob:
        next_status = "CANCELED"

    next_ts = current_ts + timedelta(seconds=random.randint(step_min, step_max))

    updates = ["status = %s"]
    params: List[object] = [next_status]

    if "updatedAt" in booking_columns:
        updates.append('"updatedAt" = %s')
        params.append(next_ts)

    if next_status != "CANCELED":
        ts_column = {
            "DRIVER_ACCEPTED": "atPickUpAt",
            "AT_PICKUP": "atPickUpAt",
            "STARTED": "startTripAt",
            "COMPLETED": "completedAt",
        }.get(next_status)
        if ts_column and ts_column in booking_columns:
            updates.insert(1, f'{pg_ident(ts_column)} = %s')
            params.insert(1, next_ts)

    params.append(booking_id)
    sql = textwrap.dedent(
        f"""
        UPDATE {pg_ident(schema)}.{pg_ident('booking')}
           SET {', '.join(updates)}
         WHERE id = %s
        """
    )
    cur.execute(sql, params)
    return next_status, next_ts


def stream_move_driver(cur, schema: str, driver_id: uuid.UUID) -> None:
    lon, lat = rand_point()
    availability = random.choice(["AVAILABLE", "BUSY", "UNAVAILABLE"])
    cur.execute(
        textwrap.dedent(
            f"""
            UPDATE {pg_ident(schema)}.{pg_ident('driver_location')}
               SET coordinates = ST_SetSRID(ST_MakePoint(%s, %s), 4326),
                   availability = %s,
                   "updatedAt" = now()
             WHERE "driverId" = %s
            """
        ),
        (lon, lat, availability, driver_id),
    )
    if cur.rowcount == 0:
        cur.execute(
            textwrap.dedent(
                f"""
                INSERT INTO {pg_ident(schema)}.{pg_ident('driver_location')} (
                  id, "driverId", coordinates, availability, "createdAt", "updatedAt"
                ) VALUES (
                  %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326), %s, now(), now()
                )
                """
            ),
            (uuid.uuid4(), driver_id, lon, lat, availability),
        )


def run_streaming_mode(args: argparse.Namespace, db_cfg: Mapping[str, object]) -> None:
    db_cfg_local = dict(db_cfg)
    conn = connect(db_cfg_local)
    try:
        column_cache = fetch_table_columns(conn, args.pg_schema)
        booking_columns = column_cache.get("booking", [])
        drivers = ensure_min_entities(
            conn,
            args.pg_schema,
            "driver",
            args.stream_drivers,
            build_driver,
            column_cache,
        )
        passengers = ensure_min_entities(
            conn,
            args.pg_schema,
            "passenger",
            args.stream_passengers,
            build_passenger,
            column_cache,
        )
        ensure_driver_locations(conn, args.pg_schema, drivers, column_cache)

        if not drivers or not passengers:
            raise RuntimeError("Streaming mode requires at least one driver and one passenger")

        print_header(
            args.volume,
            VOLUME_PRESETS.get(args.volume, VOLUME_PRESETS["small"]),
            db_cfg_local,
            args.pg_schema,
        )
        print(
            "[stream] config: "
            f"drivers>={args.stream_drivers} passengers>={args.stream_passengers} "
            f"max_inflight={args.stream_max_inflight} new_prob={args.stream_new_booking_prob:.2f} "
            f"driver_updates<= {args.stream_driver_updates} cancel_prob={args.stream_cancel_prob:.2f} "
            f"sleep=[{args.stream_sleep_min:.2f},{args.stream_sleep_max:.2f}]s "
            f"step=[{args.stream_step_min},{args.stream_step_max}]s "
            f"log_every={args.stream_log_interval}",
            flush=True,
        )
        print(
            f"[stream] baseline ready → drivers={len(drivers)} passengers={len(passengers)}",
            flush=True,
        )

        cur = conn.cursor()
        inflight: MutableMapping[uuid.UUID, Tuple[str, datetime]] = {}
        iteration = 0
        last_status: Optional[str] = None
        try:
            while True:
                iteration += 1

                if (
                    len(inflight) < args.stream_max_inflight
                    and random.random() < args.stream_new_booking_prob
                    and passengers
                ):
                    passenger_id = random.choice(passengers)
                    booking_id, ts = stream_create_booking(cur, args.pg_schema, passenger_id, booking_columns)
                    inflight[booking_id] = ("REQUESTED", ts)
                    last_status = "REQUESTED"

                for booking_id in list(inflight.keys()):
                    status, ts = inflight[booking_id]
                    next_status, next_ts = stream_advance_booking(
                        cur,
                        args.pg_schema,
                        booking_id,
                        status,
                        ts,
                        args.stream_cancel_prob,
                        args.stream_step_min,
                        args.stream_step_max,
                        booking_columns,
                    )
                    last_status = next_status
                    if next_status in {"COMPLETED", "CANCELED"}:
                        inflight.pop(booking_id, None)
                    else:
                        inflight[booking_id] = (next_status, next_ts)

                if drivers:
                    updates_cap = max(0, args.stream_driver_updates)
                    if updates_cap:
                        updates = random.randint(1, updates_cap)
                        for _ in range(updates):
                            stream_move_driver(cur, args.pg_schema, random.choice(drivers))

                if iteration % max(1, args.stream_log_interval) == 0:
                    print(
                        f"[stream] iter={iteration} inflight={len(inflight)} last_status={last_status}",
                        flush=True,
                    )

                sleep_lo = min(args.stream_sleep_min, args.stream_sleep_max)
                sleep_hi = max(args.stream_sleep_min, args.stream_sleep_max)
                time.sleep(random.uniform(sleep_lo, sleep_hi))
        except KeyboardInterrupt:
            print("\n[stream] stopping ...", flush=True)
        finally:
            cur.close()
    finally:
        conn.close()


# --------------------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------------------


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Seed or stream ride-hailing OLTP data")
    parser.add_argument(
        "--mode",
        choices=["seed", "stream"],
        default="seed",
        help="Operation mode: seed a static dataset or emit streaming updates",
    )
    parser.add_argument(
        "--volume",
        choices=sorted(VOLUME_PRESETS.keys()),
        default="small",
        help="Synthetic data volume profile for seed mode",
    )
    parser.add_argument("--pg-host", default=DEFAULT_DB["host"], help="Postgres host")
    parser.add_argument("--pg-port", type=int, default=DEFAULT_DB["port"], help="Postgres port")
    parser.add_argument("--pg-db", default=DEFAULT_DB["dbname"], help="Postgres database name")
    parser.add_argument("--pg-user", default=DEFAULT_DB["user"], help="Postgres user")
    parser.add_argument("--pg-password", default=DEFAULT_DB["password"], help="Postgres password")
    parser.add_argument("--pg-schema", default=os.getenv("PGSCHEMA", "public"), help="Target schema")
    parser.add_argument(
        "--skip-reset",
        action="store_true",
        help="Do not truncate tables before inserting (append-only) in seed mode",
    )

    stream = parser.add_argument_group("Streaming mode options")
    stream.add_argument(
        "--stream-drivers",
        type=int,
        default=None,
        help="Ensure at least this many drivers exist before streaming (preset per volume)",
    )
    stream.add_argument(
        "--stream-passengers",
        type=int,
        default=None,
        help="Ensure at least this many passengers exist before streaming (preset per volume)",
    )
    stream.add_argument(
        "--stream-max-inflight",
        type=int,
        default=None,
        help="Maximum concurrent bookings being progressed in streaming mode (preset per volume)",
    )
    stream.add_argument(
        "--stream-new-booking-prob",
        type=float,
        default=None,
        help="Probability of creating a new booking each iteration (preset per volume)",
    )
    stream.add_argument(
        "--stream-driver-updates",
        type=int,
        default=None,
        help="Maximum driver location updates per iteration (preset per volume)",
    )
    stream.add_argument(
        "--stream-cancel-prob",
        type=float,
        default=None,
        help="Cancellation probability during REQUESTED/DRIVER_ACCEPTED states (preset per volume)",
    )
    stream.add_argument(
        "--stream-sleep-min",
        type=float,
        default=None,
        help="Minimum sleep seconds between iterations (preset per volume)",
    )
    stream.add_argument(
        "--stream-sleep-max",
        type=float,
        default=None,
        help="Maximum sleep seconds between iterations (preset per volume)",
    )
    stream.add_argument(
        "--stream-step-min",
        type=int,
        default=None,
        help="Minimum seconds between booking status transitions (preset per volume)",
    )
    stream.add_argument(
        "--stream-step-max",
        type=int,
        default=None,
        help="Maximum seconds between booking status transitions (preset per volume)",
    )
    stream.add_argument(
        "--stream-log-interval",
        type=int,
        default=None,
        help="Print streaming stats every N iterations (preset per volume)",
    )
    stream.add_argument(
        "--stream-rps",
        type=float,
        default=None,
        help="Xấp xỉ số booking mới mỗi giây; được ánh xạ sang xác suất tạo booking",
    )
    stream.add_argument(
        "--driver-count",
        type=int,
        default=None,
        help="Alias cho --stream-drivers (giữ vì các script cũ)",
    )
    stream.add_argument(
        "--passenger-count",
        type=int,
        default=None,
        help="Alias cho --stream-passengers",
    )
    stream.add_argument(
        "--booking-parallelism",
        type=int,
        default=None,
        help="Alias cho --stream-max-inflight",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)
    preset = VOLUME_PRESETS[args.volume]
    db_cfg = {
        "host": args.pg_host,
        "port": args.pg_port,
        "dbname": args.pg_db,
        "user": args.pg_user,
        "password": args.pg_password,
    }

    if args.mode == "stream":
        preset = STREAM_PRESETS.get(args.volume, STREAM_PRESETS["small"])
        alias_overrides = {}
        if args.driver_count is not None:
            alias_overrides["stream_drivers"] = args.driver_count
        if args.passenger_count is not None:
            alias_overrides["stream_passengers"] = args.passenger_count
        if args.booking_parallelism is not None:
            alias_overrides["stream_max_inflight"] = args.booking_parallelism
        for key, value in preset.items():
            if getattr(args, key) is None:
                setattr(args, key, value)
        for key, value in alias_overrides.items():
            setattr(args, key, value)
        if args.stream_rps is not None:
            avg_sleep = max(0.001, (args.stream_sleep_min + args.stream_sleep_max) / 2.0)
            args.stream_new_booking_prob = max(0.0, min(0.999, args.stream_rps * avg_sleep))
            print(
                f"[stream] target_rps≈{args.stream_rps:.2f} ⇒ new_booking_prob={args.stream_new_booking_prob:.3f} "
                f"(avg_sleep≈{avg_sleep:.3f}s)"
            )
        run_streaming_mode(args, db_cfg)
        return

    print_header(args.volume, preset, db_cfg, args.pg_schema)
    print("🚀 Generating base entities ...")
    drivers, passengers = generate_entities(preset)
    print("🚗 Generating ride + booking (consistent money/keys) ...")
    rides, bookings, driver_trips, cancellations, driver_earnings = generate_rides_and_bookings(
        preset, drivers, passengers
    )
    print("💰 Generating accounts/transactions/history ...")
    accounts, histories, transactions = generate_accounts(drivers, driver_earnings)
    print("📈 Generating driver side tables ...")
    driver_locations = generate_driver_locations(drivers)
    availability_logs = generate_driver_availability(drivers)
    print("👤 Generating passenger addresses + cancellations ...")
    passenger_addresses = generate_passenger_addresses(passengers)

    conn = connect(db_cfg)
    try:
        seed_postgres(
            conn,
            args.pg_schema,
            drivers,
            passengers,
            rides,
            bookings,
            driver_trips,
            cancellations,
            driver_earnings,
            driver_locations,
            availability_logs,
            accounts,
            histories,
            transactions,
            passenger_addresses,
            args.skip_reset,
        )
    finally:
        conn.close()


if __name__ == "__main__":  # pragma: no cover - CLI entrypoint
    main()