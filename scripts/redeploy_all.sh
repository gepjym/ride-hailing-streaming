docker compose down -v || true
ok "Đã down -v"

# 2) LÊN LẠI STACK
info "docker compose up -d ..."
docker compose up -d
ok "Đã up -d"

# 3) CHỜ CÁC DỊCH VỤ SẴN SÀNG
wait_for_log postgres-source    "database system is ready to accept connections" 180
wait_for_log postgres-reporting "database system is ready to accept connections" 180
wait_for_http "http://localhost:9200" 180
wait_for_http "http://localhost:8083/connectors" 180

# 4) TẠO MAPPING ES CHO driver_locations (geo_point)
info "Tạo mapping Elasticsearch cho index ${ES_INDEX}..."
# Xoá index cũ nếu tồn tại (idempotent)
curl -s -X DELETE "http://localhost:9200/${ES_INDEX}" >/dev/null 2>&1 || true
# Tạo index mới với mapping cần thiết
curl -s -X PUT "http://localhost:9200/${ES_INDEX}" \
  -H 'Content-Type: application/json' \
  -d '{
    "mappings": { "properties": {
      "driverId":    { "type": "keyword" },
      "availability":{ "type": "keyword" },
      "serviceType": { "type": "keyword" },
      "serviceTier": { "type": "keyword" },
      "areaCode":    { "type": "keyword" },
      "@timestamp":  { "type": "date"    },
      "location":    { "type": "geo_point" }
    } }
  }' >/dev/null

# Tạo index template cho timeseries driver_locations_timeseries-*
info "Tạo index template cho driver_locations_timeseries-* ..."
curl -s -X PUT "http://localhost:9200/_index_template/driver_locations_timeseries" \
  -H 'Content-Type: application/json' \
  -d '{
    "index_patterns": ["driver_locations_timeseries-*"] ,
    "template": {
      "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0
      },
      "mappings": {
        "properties": {
          "driverId":    { "type": "keyword" },
          "availability":{ "type": "keyword" },
          "serviceType": { "type": "keyword" },
          "serviceTier": { "type": "keyword" },
          "areaCode":    { "type": "keyword" },
          "@timestamp":  { "type": "date"    },
          "location":    { "type": "geo_point" }
        }
      }
    }
  }' >/dev/null
ok "Mapping ES đã sẵn sàng"

# 5) ĐĂNG KÝ DEBEZIUM CONNECTOR
info "Đăng ký Kafka Connect Debezium..."
# Xoá connector cũ (nếu có) cho sạch
curl -s -X DELETE "http://localhost:8083/connectors/${CONNECTOR_NAME}" >/dev/null 2>&1 || true
# Đăng ký lại từ file cấu hình
if [[ -f "connectors/pg-source-connector.json" ]]; then
  curl -s -X POST "http://localhost:8083/connectors" \
    -H 'Content-Type: application/json' \
    --data @"connectors/pg-source-connector.json" >/dev/null
else
  err "Thiếu file connectors/pg-source-connector.json"; exit 1
fi
sleep 2
curl -s "http://localhost:8083/connectors/${CONNECTOR_NAME}/status" | sed -n '1,120p'
ok "Connector đã đăng ký (kỳ vọng RUNNING)"

# 6) BUILD FLINK JAR
info "Build Flink JAR..."
pushd flink-job >/dev/null
mvn -U -DskipTests clean package
popd >/dev/null
HOST_JAR="$(ls -1t "$ROOT/flink-job/target/"*.jar 2>/dev/null | head -n1 || true)"
[[ -n "$HOST_JAR" ]] || { err "Không tìm thấy JAR trong flink-job/target. Build có thể fail."; exit 1; }
@@ -125,58 +153,79 @@ JAR_BASENAME="$(basename "$HOST_JAR")"
JAR_IN_CONTAINER="/opt/flink/usrlib/${JAR_BASENAME}"

info "Copy JAR vào $JAR_IN_CONTAINER ..."
docker cp "$HOST_JAR" "flink-jobmanager:${JAR_IN_CONTAINER}" || { err "docker cp thất bại"; exit 1; }

# xác minh JAR tồn tại
info "Xác minh JAR trong container..."
docker exec -i flink-jobmanager sh -lc "ls -lh /opt/flink/usrlib || true"
if ! docker exec -i flink-jobmanager sh -lc "test -f '$JAR_IN_CONTAINER'"; then
  err "Không thấy $JAR_IN_CONTAINER trong container"; exit 1;
fi
ok "Đã có JAR: $JAR_IN_CONTAINER"

# 8) HUỶ JOB CŨ (NẾU CÓ) & SUBMIT JOB MỚI
info "Huỷ job cũ (nếu RUNNING)..."
if docker exec -i flink-jobmanager flink list | grep -q "RUNNING"; then
  OLD_ID=$(docker exec -i flink-jobmanager flink list | awk '/RUNNING/{print $4}')
  docker exec -i flink-jobmanager flink cancel "$OLD_ID" || true
fi

info "Submit job: ${FLINK_MAIN_CLASS} với $JAR_IN_CONTAINER"
docker exec -i flink-jobmanager flink run -d -c "${FLINK_MAIN_CLASS}" "${JAR_IN_CONTAINER}" \
  || { err "Submit job thất bại. Kiểm tra lại $JAR_IN_CONTAINER"; exit 1; }

docker exec -i flink-jobmanager flink list
ok "Đã submit Flink job thành công"
docker exec -it kafka kafka-console-consumer --bootstrap-server kafka:19092 \
  --topic ridehailing.public.booking --from-beginning --max-messages 3

# 9) (TUỲ CHỌN) CHẠY DATA GENERATOR
if [[ "${RUN_GENERATOR}" == "true" ]]; then
  info "Cài lib & chạy data generator..."
  python3 -m pip install --upgrade pip >/dev/null
  python3 -m pip install -q psycopg2-binary Faker >/dev/null
  if (( GENERATOR_SECONDS > 0 )); then
    info "Generator chạy ${GENERATOR_SECONDS}s..."
    timeout "${GENERATOR_SECONDS}" python3 generator/data_generator.py || true
  else
    info "Generator đang chạy (Ctrl+C để dừng ở cửa sổ này)..."
    python3 generator/data_generator.py
  fi
fi

# 10) KIỂM TRA NHANH ES & REPORTING
info "Kiểm tra nhanh Elasticsearch..."
curl -s "http://localhost:9200/${ES_INDEX}/_count" && echo
curl -s "http://localhost:9200/${ES_INDEX}/_search?size=1" | sed -n '1,120p' || true

run_reporting_query(){
  local sql="$1"
  local host_conn_failed=false

  if command -v psql >/dev/null 2>&1; then
    if PGPASSWORD=password psql "host=localhost port=5433 dbname=reporting_db user=user" -c "$sql"; then
      return 0
    else
      host_conn_failed=true
      info "Kết nối localhost:5433 thất bại, thử chạy psql trong container..."
    fi
  fi

  if command -v docker >/dev/null 2>&1; then
    PGPASSWORD=password docker exec -i postgres-reporting psql -U user -d reporting_db -c "$sql" || {
      $host_conn_failed || err "Không thể chạy truy vấn báo cáo";
      return 1
    }
  elif ! $host_conn_failed; then
    err "psql không tồn tại và không thể dùng docker exec để truy vấn báo cáo"
    return 1
  else
    return 1
  fi
}

info "Kiểm tra nhanh Reporting (latest KPI + fact phút)..."
run_reporting_query "SELECT metric_name, service_type, service_tier, area_code, metric_value, numerator_value, denominator_value, last_updated FROM mart.latest_metric_snapshot ORDER BY last_updated DESC LIMIT 10;" || true
run_reporting_query "SELECT bucket_start, bucket_granularity, metric_name, service_type, service_tier, area_code, metric_value, numerator_value, denominator_value, sample_size FROM mart.fact_metric_bucket WHERE bucket_granularity = 'MINUTE' ORDER BY bucket_start DESC LIMIT 20;" || true

ok "DONE — Flink UI: http://localhost:8081   |   Kibana: http://localhost:5601"