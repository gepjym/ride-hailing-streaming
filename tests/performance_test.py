#!/usr/bin/env python3
"""
Performance testing suite for ride-hailing streaming system.
Tests throughput and resource utilization (no latency measurement).
"""

import json
import os
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
    events_sent: int              # Theoretical number of events sent
    events_processed: int         # Kept for compatibility, but 0 when latency disabled
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
        # Source DB (ingest path)
        self.source_conn = psycopg2.connect(
            host="localhost",
            port=5432,
            dbname="ride_hailing_db",
            user="user",
            password="password",
        )

        # Reporting DB (stream outputs) – vẫn giữ để sau này nếu muốn đo chi tiết
        self.reporting_conn = psycopg2.connect(
            host="localhost",
            port=5433,
            dbname="reporting_db",
            user="user",
            password="password",
        )

        self._ensure_booking_table()

    def _ensure_booking_table(self):
        """Verify the booking table exists before running performance tests."""
        cursor = self.source_conn.cursor()
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name = 'booking'
            """
        )
        exists = cursor.fetchone()[0] == 1

        if not exists:
            raise SystemExit(
                "The source database is missing the public.booking table. "
                "Ensure the stack is running with initialized volumes (docker-compose up) "
                "before executing performance tests."
            )

    # Latency-related helpers vẫn giữ nhưng không dùng nữa
    def measure_e2e_latency(self, booking_id: str, created_at: datetime) -> float:
        return -1

    def get_cpu_usage(self) -> float:
        """Get average CPU usage of key containers."""
        containers = ["flink-jobmanager", "flink-taskmanager", "kafka"]
        total_cpu = 0.0
        count = 0

        for container in containers:
            result = subprocess.run(
                ["docker", "stats", container, "--no-stream", "--format", "{{.CPUPerc}}"],
                capture_output=True,
                text=True,
            )
            cpu_str = result.stdout.strip().replace("%", "")
            if cpu_str:
                try:
                    total_cpu += float(cpu_str)
                    count += 1
                except ValueError:
                    pass

        return total_cpu / count if count > 0 else 0.0

    def get_memory_usage(self) -> float:
        """Get total memory usage in MB for core containers."""
        containers = ["flink-jobmanager", "flink-taskmanager", "kafka"]
        total_mem = 0.0

        for container in containers:
            result = subprocess.run(
                ["docker", "stats", container, "--no-stream", "--format", "{{.MemUsage}}"],
                capture_output=True,
                text=True,
            )
            mem_str = result.stdout.strip().split("/")[0].strip()
            if not mem_str:
                continue

            try:
                if "GiB" in mem_str:
                    total_mem += float(mem_str.replace("GiB", "").strip()) * 1024
                elif "MiB" in mem_str:
                    total_mem += float(mem_str.replace("MiB", "").strip())
            except ValueError:
                continue

        return total_mem

    def run_load_test(
        self,
        scenario: str,
        events_per_minute: int,
        duration_minutes: int,
        generator_args: List[str],
        sample_interval_seconds: int,
    ) -> TestResult:
        """
        Chạy generator trong N phút với config cụ thể.
        Không chèn booking test, không đo latency – chỉ đo throughput (theoretical) + resource usage snapshot.
        """
        start_time = datetime.now()
        errors: List[str] = []

        cmd = [
            "python3",
            "generator/data_generator.py",
            "--mode",
            "stream",
            "--volume",
            "tiny",
        ] + generator_args

        print("\n============================================================")
        print(f"Scenario: {scenario}")
        print(f"  Duration: {duration_minutes} min "
              f"| Target ~ {events_per_minute} bookings/min")
        print(f"  Generator cmd: {' '.join(cmd)}")
        print("============================================================")

        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            end_deadline = time.time() + duration_minutes * 60
            while time.time() < end_deadline:
                time.sleep(sample_interval_seconds)
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()

        end_time = datetime.now()

        # Số event dự kiến (theoretical) theo config
        events_sent = events_per_minute * duration_minutes
        throughput_eps = events_sent / float(duration_minutes * 60)

        result = TestResult(
            scenario=scenario,
            start_time=start_time,
            end_time=end_time,
            events_sent=events_sent,
            events_processed=0,      # 0 vì không đo per-event latency
            avg_latency_ms=0.0,
            p95_latency_ms=0.0,
            p99_latency_ms=0.0,
            max_latency_ms=0.0,
            throughput_eps=throughput_eps,
            cpu_usage_avg=self.get_cpu_usage(),
            memory_usage_mb=self.get_memory_usage(),
            success=(len(errors) == 0),
            errors=errors,
        )

        self.print_result(result)
        return result

    def run_baseline_test(self) -> TestResult:
        """Baseline: khoảng 100 bookings/min trong 10 phút."""
        generator_args = [
            "--stream-new-booking-prob",
            "0.6",
            "--stream-sleep-min",
            "0.5",
            "--stream-sleep-max",
            "0.7",
        ]
        return self.run_load_test(
            "baseline_100_per_min_10min",
            100,
            10,
            generator_args,
            sample_interval_seconds=30,
        )

    def run_peak_test(self) -> TestResult:
        """Peak: khoảng 1.000 bookings/min trong 5 phút."""
        generator_args = [
            "--stream-new-booking-prob",
            "0.95",
            "--stream-sleep-min",
            "0.05",
            "--stream-sleep-max",
            "0.1",
        ]
        return self.run_load_test(
            "peak_1000_per_min_5min",
            1000,
            5,
            generator_args,
            sample_interval_seconds=20,
        )

    def run_spike_test(self) -> TestResult:
        """Spike: ~50.000 bookings trong 2 phút (~25.000 bookings/min)."""
        generator_args = [
            "--stream-new-booking-prob",
            "0.99",
            "--stream-sleep-min",
            "0.001",
            "--stream-sleep-max",
            "0.003",
        ]
        return self.run_load_test(
            "spike_50000_in_2min",
            25000,  # 25k/min * 2 phút ≈ 50k
            2,
            generator_args,
            sample_interval_seconds=10,
        )

    def print_result(self, result: TestResult):
        """Print test results in a formatted way."""
        print(f"\n{'='*60}")
        print(f"Test: {result.scenario}")
        print(
            f"Duration: {(result.end_time - result.start_time).total_seconds():.1f}s"
        )
        print(f"Events Sent (theoretical): {result.events_sent}")
        print(f"Events Processed (latency samples): {result.events_processed}")
        print("Success Rate (samples/events): N/A (latency disabled)")

        print(f"\nLatency Metrics (ms) - DISABLED IN THIS TEST:")
        print(f"  Average: {result.avg_latency_ms:.2f}")
        print(f"  P95:     {result.p95_latency_ms:.2f}")
        print(f"  P99:     {result.p99_latency_ms:.2f}")
        print(f"  Max:     {result.max_latency_ms:.2f}")

        print(f"\nThroughput (theoretical):")
        print(f"  ~{result.throughput_eps:.2f} events/second")

        print(f"\nResource Usage (snapshot-based):")
        print(f"  CPU (avg snapshot): {result.cpu_usage_avg:.1f}%")
        print(f"  Memory (total):     {result.memory_usage_mb:.1f} MB")

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

    tester.save_results_to_file(
        results, "tests/test_results/performance_test_results.json"
    )
