#!/usr/bin/env python3
"""
Performance testing suite for ride-hailing streaming system.
Tests throughput, latency, and resource utilization.
"""

import json
import os
import statistics
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime
from typing import List

import psycopg2


@dataclass
class TestResult:
    scenario: str
    start_time: datetime
    end_time: datetime
    events_sent: int
    events_processed: int
    avg_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float
    max_latency_ms: float
    throughput_eps: float
    cpu_usage_avg: float
    memory_usage_mb: float
    success: bool
    errors: List[str]


class PerformanceTester:
    def __init__(self):
        self.pg_conn = psycopg2.connect(
            host="localhost",
            port=5433,
            dbname="reporting_db",
            user="user",
            password="password",
        )

    def measure_e2e_latency(self, booking_id: str, created_at: datetime) -> float:
        """Measure end-to-end latency from booking creation to reporting view."""
        max_wait = 30  # seconds
        start = time.time()

        while time.time() - start < max_wait:
            cursor = self.pg_conn.cursor()
            cursor.execute(
                """
                SELECT last_updated
                FROM mart.fact_metric_bucket
                WHERE metric_name = 'request_order'
                AND first_event_at >= %s
                AND last_updated >= %s
                LIMIT 1
                """,
                (created_at, created_at),
            )

            result = cursor.fetchone()
            if result:
                reported_at = result[0]
                latency_ms = (reported_at - created_at).total_seconds() * 1000
                return latency_ms

            time.sleep(0.5)

        return -1  # Timeout

    def _percentile(self, data: List[float], percentile: float) -> float:
        if not data:
            return 0.0
        ordered = sorted(data)
        k = (len(ordered) - 1) * percentile
        f = int(k)
        c = min(f + 1, len(ordered) - 1)
        if f == c:
            return ordered[int(k)]
        d0 = ordered[f] * (c - k)
        d1 = ordered[c] * (k - f)
        return d0 + d1

    def get_cpu_usage(self) -> float:
        """Get average CPU usage of key containers."""
        containers = ["flink-jobmanager", "flink-taskmanager", "kafka"]
        total_cpu = 0
        for container in containers:
            result = subprocess.run(
                ["docker", "stats", container, "--no-stream", "--format", "{{.CPUPerc}}"],
                capture_output=True,
                text=True,
            )
            cpu_str = result.stdout.strip().replace("%", "")
            total_cpu += float(cpu_str) if cpu_str else 0
        return total_cpu / len(containers)

    def get_memory_usage(self) -> float:
        """Get total memory usage in MB for core containers."""
        containers = ["flink-jobmanager", "flink-taskmanager", "kafka"]
        total_mem = 0
        for container in containers:
            result = subprocess.run(
                ["docker", "stats", container, "--no-stream", "--format", "{{.MemUsage}}"],
                capture_output=True,
                text=True,
            )
            mem_str = result.stdout.strip().split("/")[0].strip()
            if "GiB" in mem_str:
                total_mem += float(mem_str.replace("GiB", "")) * 1024
            elif "MiB" in mem_str:
                total_mem += float(mem_str.replace("MiB", ""))
        return total_mem

    def _record_latency_sample(self, latencies: List[float], errors: List[str], sample_index: int):
        booking_id = f"perf-sample-{sample_index}"
        created_at = datetime.now()
        cursor = self.pg_conn.cursor()
        cursor.execute(
            """
            INSERT INTO public.booking 
            (id, "bookingCode", "passengerId", status, "createdAt", "updatedAt")
            VALUES (gen_random_uuid(), %s, gen_random_uuid(), 'REQUESTED', %s, %s)
            RETURNING id, "createdAt"
            """,
            (booking_id, created_at, created_at),
        )
        actual_id, actual_created = cursor.fetchone()
        self.pg_conn.commit()

        latency = self.measure_e2e_latency(str(actual_id), actual_created)
        if latency > 0:
            latencies.append(latency)
        else:
            errors.append(f"Timeout for booking {booking_id}")

    def run_load_test(
        self,
        scenario: str,
        events_per_minute: int,
        duration_minutes: int,
        generator_args: List[str],
        sample_interval_seconds: int,
    ) -> TestResult:
        start_time = datetime.now()
        latencies: List[float] = []
        errors: List[str] = []

        cmd = [
            "python3",
            "generator/data_generator.py",
            "--mode",
            "stream",
            "--volume",
            "tiny",
        ] + generator_args

        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            end_deadline = time.time() + duration_minutes * 60
            sample_index = 0
            while time.time() < end_deadline:
                self._record_latency_sample(latencies, errors, sample_index)
                sample_index += 1
                time.sleep(sample_interval_seconds)
        finally:
            proc.terminate()
            proc.wait()

        end_time = datetime.now()
        events_sent = events_per_minute * duration_minutes
        throughput_eps = events_sent / (duration_minutes * 60)

        result = TestResult(
            scenario=scenario,
            start_time=start_time,
            end_time=end_time,
            events_sent=events_sent,
            events_processed=len(latencies),
            avg_latency_ms=statistics.mean(latencies) if latencies else 0,
            p95_latency_ms=self._percentile(latencies, 0.95),
            p99_latency_ms=self._percentile(latencies, 0.99),
            max_latency_ms=max(latencies) if latencies else 0,
            throughput_eps=throughput_eps,
            cpu_usage_avg=self.get_cpu_usage(),
            memory_usage_mb=self.get_memory_usage(),
            success=len(errors) == 0,
            errors=errors,
        )

        self.print_result(result)
        return result

    def run_baseline_test(self) -> TestResult:
        """Baseline: 100 bookings/min for 10 minutes."""
        generator_args = [
            "--stream-new-booking-prob",
            "0.6",
            "--stream-sleep-min",
            "0.5",
            "--stream-sleep-max",
            "0.7",
        ]
        return self.run_load_test(
            "baseline_100_per_min",
            100,
            10,
            generator_args,
            sample_interval_seconds=30,
        )

    def run_peak_test(self) -> TestResult:
        """Peak: 1000 bookings/min for 5 minutes."""
        generator_args = [
            "--stream-new-booking-prob",
            "0.95",
            "--stream-sleep-min",
            "0.05",
            "--stream-sleep-max",
            "0.1",
        ]
        return self.run_load_test(
            "peak_1000_per_min",
            1000,
            5,
            generator_args,
            sample_interval_seconds=20,
        )

    def run_spike_test(self) -> TestResult:
        """Spike: 2000 bookings/min for 2 minutes."""
        generator_args = [
            "--stream-new-booking-prob",
            "0.98",
            "--stream-sleep-min",
            "0.02",
            "--stream-sleep-max",
            "0.05",
        ]
        return self.run_load_test(
            "spike_2000_per_min",
            2000,
            2,
            generator_args,
            sample_interval_seconds=10,
        )

    def print_result(self, result: TestResult):
        """Print test results in a formatted way."""
        print(f"\n{'='*60}")
        print(f"Test: {result.scenario}")
        print(f"Duration: {(result.end_time - result.start_time).total_seconds():.1f}s")
        print(f"Events Sent: {result.events_sent}")
        print(f"Events Processed: {result.events_processed}")
        if result.events_sent:
            print(f"Success Rate: {result.events_processed/result.events_sent*100:.1f}%")
        print(f"\nLatency Metrics:")
        print(f"  Average: {result.avg_latency_ms:.2f}ms")
        print(f"  P95: {result.p95_latency_ms:.2f}ms")
        print(f"  P99: {result.p99_latency_ms:.2f}ms")
        print(f"  Max: {result.max_latency_ms:.2f}ms")
        print(f"\nResource Usage:")
        print(f"  CPU: {result.cpu_usage_avg:.1f}%")
        print(f"  Memory: {result.memory_usage_mb:.1f}MB")
        print(f"\nStatus: {'✅ PASS' if result.success else '❌ FAIL'}")
        if result.errors:
            print(f"Errors ({len(result.errors)}):")
            for err in result.errors[:5]:
                print(f"  - {err}")
        print(f"{'='*60}\n")

    def save_results_to_file(self, results: List[TestResult], filename: str):
        """Save results to JSON for thesis."""
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        data = []
        for r in results:
            data.append(
                {
                    "scenario": r.scenario,
                    "start_time": r.start_time.isoformat(),
                    "end_time": r.end_time.isoformat(),
                    "events_sent": r.events_sent,
                    "events_processed": r.events_processed,
                    "avg_latency_ms": r.avg_latency_ms,
                    "p95_latency_ms": r.p95_latency_ms,
                    "p99_latency_ms": r.p99_latency_ms,
                    "max_latency_ms": r.max_latency_ms,
                    "throughput_eps": r.throughput_eps,
                    "cpu_usage_avg": r.cpu_usage_avg,
                    "memory_usage_mb": r.memory_usage_mb,
                    "success": r.success,
                    "errors": r.errors,
                }
            )

        with open(filename, "w") as f:
            json.dump(data, f, indent=2)

        print(f"Results saved to {filename}")


if __name__ == "__main__":
    tester = PerformanceTester()

    results: List[TestResult] = []
    results.append(tester.run_baseline_test())
    results.append(tester.run_peak_test())
    results.append(tester.run_spike_test())

    tester.save_results_to_file(results, "tests/test_results/performance_test_results.json")
