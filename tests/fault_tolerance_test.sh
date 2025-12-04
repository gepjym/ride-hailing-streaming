#!/bin/bash
set -e

print_summary_line() {
    local name=$1
    local code=$2
    if [[ $code -eq 0 ]]; then
        echo "$name: ✅ PASS"
    else
        echo "$name: ❌ FAIL"
    fi
}

test_kafka_broker_failure() {
    echo -e "\n[Test 1] Kafka broker failure recovery"

    echo "1.1. Checking baseline health..."
    curl -sf http://localhost:8081/jobs >/dev/null || return 1

    echo "1.2. Stopping Kafka broker..."
    docker stop kafka

    echo "1.3. Waiting 30 seconds..."
    sleep 30

    echo "1.4. Restarting Kafka broker..."
    docker start kafka
    sleep 20

    echo "1.5. Verifying Flink job still running..."
    job_status=$(curl -s http://localhost:8081/jobs | jq -r '.jobs[0].status')
    if [[ "$job_status" == "RUNNING" ]]; then
        echo "✅ PASS: Flink job recovered"
        return 0
    else
        echo "❌ FAIL: Flink job status: $job_status"
        return 1
    fi
}

test_flink_taskmanager_failure() {
    echo -e "\n[Test 2] Flink TaskManager failure recovery"

    echo "2.1. Killing TaskManager..."
    docker kill flink-taskmanager

    echo "2.2. Waiting for restart..."
    sleep 15

    echo "2.3. Restarting TaskManager..."
    docker-compose up -d flink-taskmanager
    sleep 20

    job_status=$(curl -s http://localhost:8081/jobs | jq -r '.jobs[0].status')
    if [[ "$job_status" == "RUNNING" ]]; then
        echo "✅ PASS: Job recovered after TaskManager restart"
        return 0
    else
        echo "❌ FAIL: Job status: $job_status"
        return 1
    fi
}

test_database_connection_loss() {
    echo -e "\n[Test 3] Database connection loss recovery"

    echo "3.1. Stopping reporting database..."
    docker stop postgres-reporting

    echo "3.2. Generating events (will be buffered)..."
    python3 generator/data_generator.py --mode stream --volume tiny &
    GENERATOR_PID=$!
    sleep 30
    kill $GENERATOR_PID

    echo "3.3. Restarting database..."
    docker start postgres-reporting
    sleep 20

    echo "3.4. Checking if data was persisted..."
    count=$(docker exec postgres-reporting psql -U user -d reporting_db -tAc \
        "SELECT COUNT(*) FROM mart.fact_metric_bucket WHERE last_updated >= NOW() - INTERVAL '2 minutes'")

    if [[ $count -gt 0 ]]; then
        echo "✅ PASS: Buffered data was persisted ($count records)"
        return 0
    else
        echo "❌ FAIL: No data found"
        return 1
    fi
}

main() {
    echo "=== FAULT TOLERANCE TEST SUITE ==="

    test_kafka_broker_failure
    kafka_result=$?

    test_flink_taskmanager_failure
    flink_result=$?

    test_database_connection_loss
    db_result=$?

    echo -e "\n=== TEST SUMMARY ==="
    print_summary_line "Kafka broker failure" $kafka_result
    print_summary_line "Flink TaskManager failure" $flink_result
    print_summary_line "Database connection loss" $db_result

    if [[ $kafka_result -eq 0 && $flink_result -eq 0 && $db_result -eq 0 ]]; then
        exit 0
    else
        exit 1
    fi
}

main "$@"
