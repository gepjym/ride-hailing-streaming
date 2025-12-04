#!/usr/bin/env python3
"""
Correctness testing: verify stream processing results match expected values
"""

import json
import time
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

import psycopg2


class CorrectnessValidator:
    def __init__(self):
        self.source_conn = psycopg2.connect(
            host="localhost",
            port=5432,
            dbname="ride_hailing_db",
            user="user",
            password="password",
        )
        self.reporting_conn = psycopg2.connect(
            host="localhost",
            port=5433,
            dbname="reporting_db",
            user="user",
            password="password",
        )
        self.results = {}

    def test_request_order_count(self, time_window_minutes: int = 60):
        """Verify request_order metric matches booking count in source DB."""
        print(f"\n=== Testing request_order count (last {time_window_minutes} min) ===")

        source_cursor = self.source_conn.cursor()
        cutoff = datetime.now() - timedelta(minutes=time_window_minutes)
        source_cursor.execute(
            """
            SELECT COUNT(*)
            FROM booking
            WHERE "createdAt" >= %s
            AND status != 'DRAFT'
            """,
            (cutoff,),
        )
        expected_count = source_cursor.fetchone()[0]

        reporting_cursor = self.reporting_conn.cursor()
        reporting_cursor.execute(
            """
            SELECT SUM(metric_value)::INTEGER
            FROM mart.fact_metric_bucket
            WHERE metric_name = 'request_order'
            AND bucket_start >= %s
            AND bucket_granularity = 'MINUTE'
            """,
            (cutoff,),
        )
        actual_count = reporting_cursor.fetchone()[0] or 0

        diff = abs(expected_count - actual_count)
        diff_pct = (diff / expected_count * 100) if expected_count > 0 else 0

        print(f"Expected (source DB): {expected_count}")
        print(f"Actual (reporting): {actual_count}")
        print(f"Difference: {diff} ({diff_pct:.2f}%)")

        passed = diff_pct <= 1.0
        print(f"Result: {'✅ PASS' if passed else '❌ FAIL'}")
        self.results["request_order_count"] = {"passed": passed, "diff_pct": diff_pct}
        return passed

    def test_acceptance_rate_formula(self):
        """Verify acceptance_rate = accept_order / request_order."""
        print("\n=== Testing acceptance_rate formula ===")

        reporting_cursor = self.reporting_conn.cursor()
        reporting_cursor.execute(
            """
            WITH metrics AS (
                SELECT 
                    bucket_start,
                    SUM(CASE WHEN metric_name = 'request_order' THEN metric_value ELSE 0 END) as requests,
                    SUM(CASE WHEN metric_name = 'accept_order' THEN metric_value ELSE 0 END) as accepts
                FROM mart.fact_metric_bucket
                WHERE bucket_granularity = 'HOUR'
                AND bucket_start >= NOW() - INTERVAL '24 hours'
                GROUP BY bucket_start
            ),
            calculated AS (
                SELECT 
                    bucket_start,
                    CASE WHEN requests > 0 
                         THEN accepts::DECIMAL / requests 
                         ELSE 0 
                    END as calculated_rate
                FROM metrics
            ),
            reported AS (
                SELECT 
                    bucket_start,
                    metric_value as reported_rate
                FROM mart.fact_metric_bucket
                WHERE metric_name = 'acceptance_rate'
                AND bucket_granularity = 'HOUR'
                AND bucket_start >= NOW() - INTERVAL '24 hours'
            )
            SELECT 
                c.bucket_start,
                c.calculated_rate,
                r.reported_rate,
                ABS(c.calculated_rate - r.reported_rate) as diff
            FROM calculated c
            JOIN reported r ON c.bucket_start = r.bucket_start
            ORDER BY diff DESC
            LIMIT 10
            """
        )

        results = reporting_cursor.fetchall()
        max_diff = max([row[3] for row in results]) if results else Decimal("0")

        print(f"Max difference: {max_diff:.4f}")
        print("Top 5 discrepancies:")
        for row in results[:5]:
            print(
                f"  {row[0]}: calculated={row[1]:.4f}, "
                f"reported={row[2]:.4f}, diff={row[3]:.4f}"
            )

        passed = max_diff <= Decimal("0.01")
        print(f"Result: {'✅ PASS' if passed else '❌ FAIL'}")
        self.results["acceptance_rate_formula"] = {
            "passed": passed,
            "max_diff": float(max_diff),
        }
        return passed

    def test_gsv_revenue_relationship(self):
        """
        Verify: GSV = Revenue_VAT + Discount
                Revenue_Net = Revenue_VAT / 1.1
        """
        print("\n=== Testing GSV & Revenue formulas ===")

        reporting_cursor = self.reporting_conn.cursor()
        reporting_cursor.execute(
            """
            WITH metrics AS (
                SELECT 
                    bucket_start,
                    SUM(CASE WHEN metric_name = 'gsv' THEN metric_value ELSE 0 END) as gsv,
                    SUM(CASE WHEN metric_name = 'revenue_vat' THEN metric_value ELSE 0 END) as rev_vat,
                    SUM(CASE WHEN metric_name = 'revenue_net' THEN metric_value ELSE 0 END) as rev_net,
                    SUM(CASE WHEN metric_name = 'discount_order' THEN metric_value ELSE 0 END) as discount
                FROM mart.fact_metric_bucket
                WHERE bucket_granularity = 'HOUR'
                AND bucket_start >= NOW() - INTERVAL '24 hours'
                GROUP BY bucket_start
            )
            SELECT 
                bucket_start,
                gsv,
                rev_vat,
                rev_net,
                discount,
                ABS(gsv - (rev_vat + discount)) as gsv_check,
                ABS(rev_net - (rev_vat / 1.1)) as rev_net_check
            FROM metrics
            WHERE gsv > 0
            ORDER BY gsv_check DESC
            LIMIT 10
            """
        )

        results = reporting_cursor.fetchall()
        max_gsv_diff = max([row[5] for row in results]) if results else Decimal("0")
        max_rev_diff = max([row[6] for row in results]) if results else Decimal("0")

        print(f"Max GSV formula diff: {max_gsv_diff:.2f}")
        print(f"Max Revenue_Net formula diff: {max_rev_diff:.2f}")

        passed = max_gsv_diff <= 1 and max_rev_diff <= 1
        print(f"Result: {'✅ PASS' if passed else '❌ FAIL'}")
        self.results["gsv_revenue_formula"] = {
            "passed": passed,
            "max_gsv_diff": float(max_gsv_diff),
            "max_rev_diff": float(max_rev_diff),
        }
        return passed

    def test_late_events_handling(self):
        """Insert a booking with past timestamp, verify it's processed correctly."""
        print("\n=== Testing late events handling ===")

        past_time = datetime.now() - timedelta(minutes=10)

        source_cursor = self.source_conn.cursor()
        source_cursor.execute(
            """
            INSERT INTO booking 
            (id, "bookingCode", "passengerId", status, "createdAt", "updatedAt")
            VALUES 
            (gen_random_uuid(), 'TEST-LATE-EVENT', gen_random_uuid(), 'REQUESTED', %s, %s)
            RETURNING id
            """,
            (past_time, past_time),
        )
        booking_id = source_cursor.fetchone()[0]
        self.source_conn.commit()

        print(f"Inserted late booking: {booking_id} at {past_time}")

        time.sleep(10)

        reporting_cursor = self.reporting_conn.cursor()
        reporting_cursor.execute(
            """
            SELECT COUNT(*)
            FROM mart.fact_metric_bucket
            WHERE metric_name = 'request_order'
            AND bucket_start = DATE_TRUNC('minute', %s::TIMESTAMP)
            AND metric_value > 0
            """,
            (past_time,),
        )

        found = reporting_cursor.fetchone()[0] > 0

        print(f"Late event processed: {'✅ YES' if found else '❌ NO'}")
        self.results["late_events"] = {"passed": found}
        return found

    def write_report(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w") as f:
            json.dump(self.results, f, indent=2, default=str)
        print(f"Report written to {path}")

    def run_all_tests(self):
        """Run all correctness tests."""
        print("\n" + "=" * 60)
        print("CORRECTNESS VALIDATION TEST SUITE")
        print("=" * 60)

        all_passed = True
        all_passed &= self.test_request_order_count()
        all_passed &= self.test_acceptance_rate_formula()
        all_passed &= self.test_gsv_revenue_relationship()
        all_passed &= self.test_late_events_handling()

        print("\n" + "=" * 60)
        print("SUMMARY")
        print("=" * 60)
        for name, result in self.results.items():
            status = "✅ PASS" if result.get("passed") else "❌ FAIL"
            print(f"{name}: {status}")

        total = len(self.results)
        passed = len([r for r in self.results.values() if r.get("passed")])
        print(f"\nTotal: {passed}/{total} tests passed ({passed/total*100 if total else 0:.1f}%)")

        self.write_report(Path("tests/test_results/correctness_results.json"))
        return all_passed


if __name__ == "__main__":
    validator = CorrectnessValidator()
    success = validator.run_all_tests()
    exit(0 if success else 1)
