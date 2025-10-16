#!/usr/bin/env python3
import psycopg2
from faker import Faker
import random
import time
import uuid
from datetime import datetime, timedelta

DB_CONFIG = {
    "host": "localhost",
    "port": "5432",
    "dbname": "ride_hailing_db",
    "user": "user",
    "password": "password",
}

fake = Faker()
random.seed(42)
Faker.seed(42)

SERVICE_TYPES = ["BIKE", "CAR"]
SERVICE_TIERS  = ["ECONOMY", "PREMIUM"]
AREAS = ["Q1", "Q3", "Q7", "BT", "TP"]

driver_ids = []
passenger_ids = []

def conn():
    while True:
        try:
            c = psycopg2.connect(**DB_CONFIG)
            c.autocommit = True
            return c
        except psycopg2.OperationalError as e:
            print("[wait] PostgreSQL not ready:", e)
            time.sleep(3)

def reset(cursor):
    cursor.execute("""
        TRUNCATE TABLE booking RESTART IDENTITY;
        TRUNCATE TABLE driver_location RESTART IDENTITY;
        TRUNCATE TABLE driver RESTART IDENTITY;
        TRUNCATE TABLE passenger RESTART IDENTITY;
    """)

def seed_drivers(cursor, n=40):
    global driver_ids
    for _ in range(n):
        did = str(uuid.uuid4())
        driver_ids.append(did)
        cursor.execute("""
            INSERT INTO driver (id, "fullName", email, "phoneNumber", "countryCode",
                                status, "password", "areaCode", "serviceCode")
            VALUES (%s, %s, %s, %s, %s, 'ACTIVE', 'hashed', %s, %s)
        """, (
            did, fake.name(), fake.email(),
            random.randint(100000000, 999999999), 'VN',
            random.choice(AREAS), random.choice(SERVICE_TYPES)
        ))
        lon, lat = random.uniform(106.6, 106.9), random.uniform(10.7, 11.0)
        cursor.execute("""
            INSERT INTO driver_location (id, "driverId", coordinates, availability)
            VALUES (%s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326), 'AVAILABLE')
        """, (str(uuid.uuid4()), did, lon, lat))

def seed_passengers(cursor, n=60):
    global passenger_ids
    for _ in range(n):
        pid = str(uuid.uuid4())
        passenger_ids.append(pid)
        cursor.execute("""
            INSERT INTO passenger (id, "fullName", email, "phoneNumber", "countryCode", status, "password")
            VALUES (%s, %s, %s, %s, %s, 'ACTIVE', 'hashed')
        """, (
            pid, fake.name(), fake.email(),
            random.randint(1000000000, 9999999999), 'VN'
        ))

def new_booking(cursor):
    pid = random.choice(passenger_ids)
    service_type = random.choice(SERVICE_TYPES)
    service_tier  = random.choice(SERVICE_TIERS)
    area          = random.choice(AREAS)

    lon1, lat1 = random.uniform(106.6, 106.9), random.uniform(10.7, 11.0)
    lon2, lat2 = lon1 + random.uniform(-0.02, 0.02), lat1 + random.uniform(-0.02, 0.02)

    bid = str(uuid.uuid4())
    now = datetime.utcnow()

    cursor.execute("""
        INSERT INTO booking (
          id, "passengerId", status,
          "serviceTypeCode", "serviceTierCode",
          "pickupAddress",  "dropoffAddress",
          "pickupLocation", "dropoffLocation",
          "wardCode",
          "discountAmount", "platformFee",
          "createdAt", "updatedAt"
        )
        VALUES (
          %s, %s, 'REQUESTED',
          %s, %s,
          %s, %s,
          ST_SetSRID(ST_MakePoint(%s,%s), 4326),
          ST_SetSRID(ST_MakePoint(%s,%s), 4326),
          %s,
          %s, %s,
          %s, %s
        )
    """, (
        bid, pid,
        service_type, service_tier,
        "Pickup", "Dropoff",
        lon1, lat1, lon2, lat2,
        area,
        random.randint(0, 20000),
        random.randint(20000, 100000),
        now, now
    ))
    return bid, service_type, service_tier, area, now

def advance_booking(cursor, bid, current_status, now):
    transitions = {
        "REQUESTED": "DRIVER_ACCEPTED",
        "DRIVER_ACCEPTED": "AT_PICKUP",
        "AT_PICKUP": "STARTED",
        "STARTED": "COMPLETED"
    }
    cancel_prob = 0.12
    next_status = transitions.get(current_status, "COMPLETED")
    if current_status in ("REQUESTED", "DRIVER_ACCEPTED") and random.random() < cancel_prob:
        next_status = "CANCELED"

    ts_col = {
        "DRIVER_ACCEPTED": '"atPickUpAt"',
        "AT_PICKUP": '"atPickUpAt"',
        "STARTED": '"startTripAt"',
        "COMPLETED": '"completedAt"',
        "CANCELED": '"updatedAt"'
    }.get(next_status, '"updatedAt"')

    t = now + timedelta(seconds=random.randint(5, 25))

    if ts_col == '"updatedAt"':
        # Tránh set duplicated column
        cursor.execute(
            'UPDATE booking SET status = %s, "updatedAt" = %s WHERE id = %s',
            (next_status, t, bid)
        )
    else:
        cursor.execute(
            f'UPDATE booking SET status = %s, {ts_col} = %s, "updatedAt" = %s WHERE id = %s',
            (next_status, t, t, bid)
        )

    return next_status, t

def move_random_driver(cursor):
    driver_id = random.choice(driver_ids)
    lon, lat = random.uniform(106.6, 106.9), random.uniform(10.7, 11.0)
    availability = random.choice(["AVAILABLE","BUSY","UNAVAILABLE"])
    cursor.execute("""
        UPDATE driver_location
           SET coordinates = ST_SetSRID(ST_MakePoint(%s, %s), 4326),
               availability = %s,
               "updatedAt" = now()
         WHERE "driverId" = %s
    """, (lon, lat, availability, driver_id))

def main_loop():
    c = conn()
    cur = c.cursor()
    print("[init] reset & seed ...")
    reset(cur)
    seed_drivers(cur, n=30)
    seed_passengers(cur, n=80)

    print("[run] streaming events ... Ctrl+C to stop")
    inflight = {}
    try:
        while True:
            if random.random() < 0.6:
                bid, stype, tier, area, now = new_booking(cur)
                inflight[bid] = ("REQUESTED", now)

            for bid in list(inflight.keys()):
                status, ts = inflight[bid]
                if status in ("COMPLETED","CANCELED"):
                    del inflight[bid]
                    continue
                next_status, t = advance_booking(cur, bid, status, ts)
                inflight[bid] = (next_status, t)

            for _ in range(random.randint(1, 5)):
                move_random_driver(cur)

            time.sleep(random.uniform(0.8, 1.5))
    except KeyboardInterrupt:
        print("\n[stop] bye!")
    finally:
        cur.close()
        c.close()

if __name__ == "__main__":
    main_loop()
