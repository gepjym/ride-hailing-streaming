#!/usr/bin/env bash
set -euo pipefail
mkdir -p flink_libs && cd flink_libs
base="https://repo1.maven.org/maven2"
curl -LO $base/org/apache/flink/flink-connector-kafka/3.1.0-1.17/flink-connector-kafka-3.1.0-1.17.jar
curl -LO $base/org/apache/flink/flink-connector-jdbc/3.1.0-1.17/flink-connector-jdbc-3.1.0-1.17.jar
curl -LO $base/org/apache/flink/flink-connector-elasticsearch7/3.0.1-1.17/flink-connector-elasticsearch7-3.0.1-1.17.jar
curl -LO $base/org/apache/flink/flink-json/1.17.2/flink-json-1.17.2.jar
curl -LO $base/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar
curl -LO $base/org/locationtech/jts/jts-core/1.19.0/jts-core-1.19.0.jar
echo "[ok] downloaded"
