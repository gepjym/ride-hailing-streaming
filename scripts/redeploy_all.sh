#!/usr/bin/env bash
set -euo pipefail

# ================= CẤU HÌNH NHANH =================
FLINK_MAIN_CLASS="${FLINK_MAIN_CLASS:-com.ridehailing.RideHailingDataProcessor}"
CONNECTOR_NAME="${CONNECTOR_NAME:-pg-source-connector}"
CONNECTOR_FILE="${CONNECTOR_FILE:-connectors/pg-source-connector.json}"
ES_INDEX="${ES_INDEX:-driver_locations}"

RUN_GENERATOR="${RUN_GENERATOR:-false}"     # true/false
GENERATOR_SECONDS="${GENERATOR_SECONDS:-0}" # >0: tự dừng generator sau N giây
GENERATOR_MODE="${GENERATOR_MODE:-stream}"  # stream | seed
GENERATOR_VOLUME="${GENERATOR_VOLUME:-small}" # tiny | small | medium | large
GENERATOR_EXTRA_ARGS="${GENERATOR_EXTRA_ARGS:-}" # ví dụ: "--stream-max-inflight 1200"
GENERATOR_PGHOST="${GENERATOR_PGHOST:-localhost}"
GENERATOR_PGPORT="${GENERATOR_PGPORT:-5432}"
GENERATOR_PGDB="${GENERATOR_PGDB:-ride_hailing_db}"
GENERATOR_PGUSER="${GENERATOR_PGUSER:-user}"
GENERATOR_PGPASSWORD="${GENERATOR_PGPASSWORD:-password}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:19092,kafka-1:19092}"
KAFKA_CLI_CONTAINER="${KAFKA_CLI_CONTAINER:-kafka}"
ES_USERNAME="${ES_USERNAME:-elastic}"
ES_PASSWORD="${ELASTIC_PASSWORD:-changeme123}"
# kafka-topics binary will be auto-detected (Confluent vs Bitnami)
KAFKA_TOPICS_BIN="${KAFKA_TOPICS_BIN:-}"
# ================================================

ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
cd "$ROOT"

if ! command -v docker >/dev/null 2>&1; then
  echo "[FAIL] Docker chưa được cài đặt trong PATH" >&2
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  echo "[FAIL] Không thể kết nối Docker daemon. Hãy mở Docker Desktop/daemon trước khi chạy." >&2
  exit 1
fi

info(){ echo -e "[\033[1;34mINFO\033[0m] $*"; }
ok(){   echo -e "[\033[1;32m OK \033[0m] $*"; }
err(){  echo -e "[\033[1;31mFAIL\033[0m] $*" 1>&2; }

wait_for_http(){
  local url="$1" timeout="${2:-120}"
  shift 2 || true
  local curl_args=("$@")
  local start="$(date +%s)"
  info "Đợi HTTP $url ..."
  while true; do
    if [[ ${#curl_args[@]} -gt 0 ]]; then
      curl -sSf "${curl_args[@]}" "$url" >/dev/null 2>&1 && break
    else
      curl -sSf "$url" >/dev/null 2>&1 && break
    fi
    sleep 2
    (( $(date +%s) - start > timeout )) && { err "Timeout đợi $url"; return 1; }
  done
  ok "$url OK"
}

# 0) In thông tin tổng quan
info "Thư mục làm việc: $ROOT"
info "Main class: $FLINK_MAIN_CLASS"
info "Connector:  $CONNECTOR_NAME"
info "ES index:   $ES_INDEX"
info "Generator:  RUN=${RUN_GENERATOR}, MODE=${GENERATOR_MODE}, VOL=${GENERATOR_VOLUME}, SECONDS=${GENERATOR_SECONDS}"
info "Generator DB: host=${GENERATOR_PGHOST} port=${GENERATOR_PGPORT} db=${GENERATOR_PGDB}"

# 1) HẠ TOÀN BỘ STACK + XOÁ VOLUMES (RESET SẠCH)
info "docker compose down -v --remove-orphans ..."
# dọn sạch các container/broker lẻ (ví dụ 'kafka' bản cũ) để tránh kẹt network/port
docker compose down -v --remove-orphans || true
# xoá thủ công container kafka đơn lẻ nếu còn sót
docker rm -f kafka >/dev/null 2>&1 || true
ok "Đã down -v"

# helper đảm bảo container không bị pause/stop
ensure_running(){
  local container="$1"
  local state status paused
  local start_ts="$(date +%s)"

  while true; do
    state=$(docker inspect -f '{{.State.Status}} {{.State.Paused}}' "$container" 2>/dev/null || echo "unknown false")
    status="${state%% *}"
    paused="${state##* }"

    # Nếu container không tồn tại (state=unknown) thử dựng lại service tương ứng qua docker compose
    if [[ "$status" == "unknown" ]]; then
      info "Container $container chưa tồn tại → docker compose up -d $container"
      docker compose up -d "$container" >/dev/null 2>&1 || true
      sleep 2
      continue
    fi

    if [[ "$status" == "paused" || "$paused" == "true" ]]; then
      info "Container $container đang paused → unpause"
      docker unpause "$container" >/dev/null 2>&1 || true
      sleep 1
      continue
    fi

    if [[ "$status" != "running" ]]; then
      info "Khởi động container $container"
      docker start "$container" >/dev/null 2>&1 || true
      sleep 2
      # quay vòng kiểm tra lại
      if (( $(date +%s) - start_ts > 120 )); then
        err "Container $container không thể khởi động sau 120s";
        docker logs "$container" | tail -n 80 >&2 || true
        return 1
      fi
      continue
    fi

    # container đang running → thoát
    return 0
  done
}

detect_kafka_topics_bin(){
  local c="$KAFKA_CLI_CONTAINER"
  ensure_running "$c" || return 1
  if docker exec -i "$c" bash -lc 'command -v kafka-topics >/dev/null 2>&1'; then
    KAFKA_TOPICS_BIN="kafka-topics"
    return 0
  fi
  if docker exec -i "$c" bash -lc '[ -x /opt/bitnami/kafka/bin/kafka-topics.sh ]'; then
    KAFKA_TOPICS_BIN="/opt/bitnami/kafka/bin/kafka-topics.sh"
    return 0
  fi
  err "Không tìm thấy kafka-topics trong container $c"
  return 1
}

kafka_cli_pick_running(){
  local candidates=("${KAFKA_CLI_CONTAINER}" kafka kafka-1)
  for c in "${candidates[@]}"; do
    if docker inspect -f '{{.State.Running}}' "$c" 2>/dev/null | grep -q true; then
      KAFKA_CLI_CONTAINER="$c"
      export KAFKA_CLI_CONTAINER
      return 0
    fi
  done

  ensure_running "$KAFKA_CLI_CONTAINER" || true
  if docker inspect -f '{{.State.Running}}' "$KAFKA_CLI_CONTAINER" 2>/dev/null | grep -q true; then
    return 0
  fi

  err "Không tìm thấy broker Kafka nào đang RUNNING"
  docker compose ps kafka-1 kafka || true
  return 1
}

kafka_topics(){
  kafka_cli_pick_running || return 1
  ensure_running "$KAFKA_CLI_CONTAINER" || return 1
  if [[ -z "${KAFKA_TOPICS_BIN:-}" ]]; then
    detect_kafka_topics_bin || return 1
  fi
  docker exec -i "$KAFKA_CLI_CONTAINER" "$KAFKA_TOPICS_BIN" --bootstrap-server "$KAFKA_BOOTSTRAP" "$@"
}

wait_for_topics(){
  local timeout="${1:-180}"
  shift
  local required=("$@")
  if [[ ${#required[@]} -eq 0 ]]; then
    return 0
  fi
  ensure_running "$KAFKA_CLI_CONTAINER"
  local start="$(date +%s)"
  info "Đợi Kafka có các topic: ${required[*]} ..."
  while true; do
    local topic_list
    topic_list=$(kafka_topics --list 2>/dev/null || true)
    local missing=()
    local not_ready=()
    for topic in "${required[@]}"; do
      if ! grep -Fxq "$topic" <<<"$topic_list"; then
        missing+=("$topic")
        continue
      fi
      if ! kafka_topics --describe --topic "$topic" >/dev/null 2>&1; then
        not_ready+=("$topic")
      fi
    done
    if [[ ${#missing[@]} -eq 0 && ${#not_ready[@]} -eq 0 ]]; then
      ok "Kafka topics sẵn sàng"
      return 0
    fi
    if (( $(date +%s) - start > timeout )); then
      err "Timeout đợi Kafka topics: ${missing[*]} ${not_ready[*]}"
      kafka_topics --list || true
      return 1
    fi
    sleep 5
  done
}

wait_for_kafka_brokers(){
  local timeout="${1:-180}"
  local start="$(date +%s)"

  ensure_running "$KAFKA_CLI_CONTAINER" || true
  info "Đợi Kafka broker sẵn sàng ..."
  while true; do
    kafka_cli_pick_running || true
    ensure_running "$KAFKA_CLI_CONTAINER" || true
    if kafka_topics --list >/dev/null 2>&1; then
      ok "Kafka broker đã sẵn sàng"
      return 0
    fi

    if (( $(date +%s) - start > timeout )); then
      err "Timeout đợi Kafka broker sẵn sàng"
      docker logs "$KAFKA_CLI_CONTAINER" | tail -n 80 >&2 || true
      return 1
    fi

    sleep 3
  done
}

ensure_topic(){
  local topic="$1"
  local attempts=0
  info "Đảm bảo Kafka topic '${topic}' tồn tại..."
  kafka_cli_pick_running || return 1
  ensure_running "$KAFKA_CLI_CONTAINER" || return 1
  while true; do
    if kafka_topics --describe --topic "$topic" >/dev/null 2>&1; then
      ok "Topic '${topic}' đã tồn tại"
      return 0
    fi
    if kafka_topics --create --if-not-exists --topic "$topic" --partitions 3 --replication-factor 1 >/dev/null 2>&1; then
      ok "Topic '${topic}' đã tạo"
      return 0
    fi
    # nếu broker vừa khởi động lại hoặc bị stop, thử ensure_running lại trước khi retry
    kafka_cli_pick_running || true
    ensure_running "$KAFKA_CLI_CONTAINER" || true
    if (( attempts++ >= 30 )); then
      err "Không thể tạo topic '${topic}'"
      kafka_topics --list || true
      return 1
    fi
    sleep 3
  done
}

wait_for_postgres(){
  local container="$1" db="$2" user="$3" timeout="${4:-180}"
  local start="$(date +%s)"
  info "Đợi PostgreSQL $container ($db)..."
  ensure_running "$container"
  local attempt=0
  until docker exec "$container" pg_isready -d "$db" -U "$user" >/dev/null 2>&1; do
    sleep 2
    ((attempt++))
    # nếu container không chạy (ví dụ fail do conflict port), thử khởi động lại và log cảnh báo
    if ! docker inspect -f '{{.State.Running}}' "$container" 2>/dev/null | grep -q true; then
      info "Container $container chưa chạy, thử khởi động lại..."
      docker start "$container" >/dev/null 2>&1 || true
    fi
    if (( $(date +%s) - start > timeout )); then
      err "Timeout đợi $container";
      docker logs "$container" | tail -n 80 >&2 || true
      return 1
    fi
    # thỉnh thoảng in dấu chấm để tránh cảm giác đứng im
    if (( attempt % 10 == 0 )); then
      info "... vẫn chờ $container khởi động (attempt=${attempt})"
    fi
  done
  ok "$container sẵn sàng"
}

# 2) LÊN LẠI STACK
info "docker compose up -d ..."
docker compose up -d
ok "Đã up -d"

# 3) CHỜ CÁC DỊCH VỤ SẴN SÀNG
wait_for_kafka_brokers 240
wait_for_postgres postgres-source    "ride_hailing_db" "user" 240
wait_for_postgres postgres-reporting "reporting_db"   "user" 240

wait_for_http "http://localhost:9200" 180 -u "${ES_USERNAME}:${ES_PASSWORD}"
wait_for_http "http://localhost:8083/connectors" 180
wait_for_http "http://localhost:5601/api/status" 180 -u "${ES_USERNAME}:${ES_PASSWORD}"

# 4) TẠO MAPPING ES CHO driver_locations (geo_point)
info "Tạo mapping Elasticsearch cho index ${ES_INDEX}..."
# Xoá index cũ nếu tồn tại (idempotent)
curl -s -u "${ES_USERNAME}:${ES_PASSWORD}" -X DELETE "http://localhost:9200/${ES_INDEX}" >/dev/null 2>&1 || true
# Tạo index mới với mapping cần thiết
curl -s -u "${ES_USERNAME}:${ES_PASSWORD}" -X PUT "http://localhost:9200/${ES_INDEX}" \
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
curl -s -u "${ES_USERNAME}:${ES_PASSWORD}" -X PUT "http://localhost:9200/_index_template/driver_locations_timeseries" \
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

# 4b) TẠO INDEX PATTERN TRÊN KIBANA (nếu chưa có)
ensure_kibana_index_pattern(){
  local pattern="$1" title="${2:-$1}" time_field="${3:-@timestamp}"
  local search_term="${title//\*/%2A}"
  local search_url="http://localhost:5601/api/saved_objects/_find?type=index-pattern&search_fields=title&search=${search_term}"

  if curl -s -u "${ES_USERNAME}:${ES_PASSWORD}" "${search_url}" -H "kbn-xsrf: reporting-init" -H "Content-Type: application/json" \
    | grep -q '"total"[[:space:]]*:[[:space:]]*[1-9]'; then
    ok "Kibana index pattern '${title}' đã tồn tại"
    return 0
  fi

  info "Tạo Kibana index pattern '${title}'..."
  curl -s -u "${ES_USERNAME}:${ES_PASSWORD}" -X POST "http://localhost:5601/api/saved_objects/index-pattern" \
    -H "kbn-xsrf: reporting-init" \
    -H "Content-Type: application/json" \
    -d "{\"attributes\":{\"title\":\"${title}\",\"timeFieldName\":\"${time_field}\"}}" >/dev/null
  ok "Kibana index pattern '${title}' đã tạo"
}

ensure_kibana_index_pattern "${ES_INDEX}" "${ES_INDEX}" "@timestamp"
ensure_kibana_index_pattern "driver_locations_timeseries-*" "driver_locations_timeseries-*" "@timestamp"

# 5) ĐĂNG KÝ DEBEZIUM CONNECTOR
info "Đăng ký Kafka Connect Debezium..."

# Xoá connector cũ (nếu có) cho sạch
curl -s -X DELETE "http://localhost:8083/connectors/${CONNECTOR_NAME}" >/dev/null 2>&1 || true

# tìm file cấu hình (cho phép fallback nếu đổi vị trí)
CONNECTOR_FILE_CANDIDATE="${CONNECTOR_FILE}"
[[ -f "$CONNECTOR_FILE_CANDIDATE" ]] || CONNECTOR_FILE_CANDIDATE="pg-source-connector.json"
[[ -f "$CONNECTOR_FILE_CANDIDATE" ]] || { err "Không tìm thấy file connector JSON (CONNECTOR_FILE=$CONNECTOR_FILE)"; exit 1; }

# kiểm tra plugin Debezium đã có trong Kafka Connect chưa
if ! curl -s http://localhost:8083/connector-plugins | grep -q 'io.debezium.connector.postgresql.PostgresConnector'; then
  err "Kafka Connect chưa có plugin Debezium Postgres (io.debezium.connector.postgresql.PostgresConnector). Kiểm tra lại image/CONNECT_PLUGIN_PATH."
  exit 1
fi

# đăng ký connector và kiểm tra mã HTTP
resp="$(curl -s -w '\n%{http_code}' -X POST "http://localhost:8083/connectors" \
  -H 'Content-Type: application/json' \
  --data @"${CONNECTOR_FILE_CANDIDATE}")"
body="$(printf '%s' "$resp" | sed '$d')"
code="$(printf '%s' "$resp" | tail -n1)"

echo "[DEBUG] POST /connectors => HTTP $code"
echo "$body" | sed -n '1,200p'

if [[ "$code" != "201" ]]; then
  err "Kafka Connect trả lỗi khi tạo connector (HTTP $code)."
  docker compose logs --tail=200 kafka-connect >&2 || true
  exit 1
fi

# chờ connector vào trạng thái RUNNING
for attempt in {1..20}; do
  status_json="$(curl -s "http://localhost:8083/connectors/${CONNECTOR_NAME}/status" || true)"
  echo "$status_json" | sed -n '1,80p'
  if echo "$status_json" | grep -q '"state"[[:space:]]*:[[:space:]]*"RUNNING"'; then
    ok "Connector RUNNING"
    break
  fi
  sleep 3
done

if ! echo "$status_json" | grep -q '"state"[[:space:]]*:[[:space:]]*"RUNNING"'; then
  err "Connector chưa RUNNING (hoặc chưa tạo). Kiểm tra log kafka-connect."
  docker compose logs --tail=200 kafka-connect >&2 || true
  exit 1
fi

# đảm bảo các topic CDC đã được tạo trước khi Flink kết nối
ensure_topic "ridehailing.public.booking"
ensure_topic "ridehailing.public.driver"
ensure_topic "ridehailing.public.driver_location"
ensure_topic "ridehailing.public.passenger"

# chờ Debezium khởi tạo các topic CDC trước khi submit Flink job
wait_for_topics 240 \
  ridehailing.public.booking \
  ridehailing.public.driver \
  ridehailing.public.driver_location \
  ridehailing.public.passenger

# 6) BUILD FLINK JAR
info "Build Flink JAR..."
pushd flink-job >/dev/null
mvn -U -DskipTests clean package
popd >/dev/null
HOST_JAR="$(ls -1t "$ROOT/flink-job/target/"*.jar 2>/dev/null | head -n1 || true)"
[[ -n "$HOST_JAR" ]] || { err "Không tìm thấy JAR trong flink-job/target. Build có thể fail."; exit 1; }
ok "Build xong: $(basename "$HOST_JAR")"

info "Khởi động Flink JM/TM..."
docker compose up -d flink-jobmanager flink-taskmanager

# đợi JobManager ổn định
info "Chờ JobManager ổn định..."
for i in {1..30}; do
  if docker inspect -f '{{.State.Running}} {{.State.Restarting}}' flink-jobmanager 2>/dev/null | grep -q '^true false$'; then
    break
  fi
  sleep 1
done

ensure_container_path(){
  local container="$1" path="$2"
  docker exec -i "$container" sh -lc "mkdir -p '$path' && chown -R flink:flink '$path'"
}

wait_for_job_running(){
  local job_id="$1" timeout="${2:-120}"
  local start="$(date +%s)"
  info "Chờ job ${job_id} chuyển sang RUNNING..."
  while true; do
    local status_line
    status_line=$(docker exec -i flink-jobmanager flink list | grep "$job_id" || true)
    if [[ -n "$status_line" ]]; then
      if [[ "$status_line" == *"(RUNNING)"* ]]; then
        ok "Job ${job_id} đang RUNNING"
        return 0
      fi
      if [[ "$status_line" == *"(FAILED)"* || "$status_line" == *"(FAILING)"* || "$status_line" == *"(CANCELED)"* ]]; then
      err "Job ${job_id} gặp lỗi: ${status_line}"
      docker logs --tail 200 flink-jobmanager || true
      return 1
      fi
    fi
    if (( $(date +%s) - start > timeout )); then
      err "Job ${job_id} chưa RUNNING sau ${timeout}s"
      docker exec -i flink-jobmanager flink list || true
      docker logs --tail 200 flink-jobmanager || true
      return 1
    fi
    sleep 5
  done
}

# chuẩn bị thư mục lưu checkpoint/savepoint và usrlib
info "Chuẩn bị thư mục Flink trong container..."
ensure_container_path flink-jobmanager /opt/flink/usrlib
ensure_container_path flink-jobmanager /flink-checkpoints
ensure_container_path flink-taskmanager /flink-checkpoints

# copy JAR vào usrlib
HOST_JAR="${HOST_JAR:?missing host jar}"
JAR_BASENAME="$(basename "$HOST_JAR")"
JAR_IN_CONTAINER="/opt/flink/usrlib/${JAR_BASENAME}"

TMP_JAR_PATH="/tmp/${JAR_BASENAME}"

info "Copy JAR vào $TMP_JAR_PATH rồi di chuyển tới $JAR_IN_CONTAINER ..."
docker cp "$HOST_JAR" "flink-jobmanager:${TMP_JAR_PATH}" || { err "docker cp thất bại"; exit 1; }
docker exec -i flink-jobmanager sh -lc "mkdir -p /opt/flink/usrlib && mv '${TMP_JAR_PATH}' '${JAR_IN_CONTAINER}'"

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
JOB_SUBMIT_OUTPUT=$(docker exec -i flink-jobmanager flink run -d -c "${FLINK_MAIN_CLASS}" "${JAR_IN_CONTAINER}" 2>&1) \
  || { err "Submit job thất bại. Kiểm tra lại $JAR_IN_CONTAINER"; echo "$JOB_SUBMIT_OUTPUT" >&2; exit 1; }
echo "$JOB_SUBMIT_OUTPUT"
JOB_ID=$(echo "$JOB_SUBMIT_OUTPUT" | awk '/Job has been submitted with JobID/ {print $NF}' | tail -n1)
if [[ -n "$JOB_ID" ]]; then
  wait_for_job_running "$JOB_ID" 180
else
  info "Không thể lấy JobID từ log submit, bỏ qua bước đợi RUNNING"
fi

docker exec -i flink-jobmanager flink list
ok "Đã submit Flink job thành công"

if [[ "${RUN_GENERATOR}" != "true" ]]; then
  info "Smoke test topic booking (timeout 5s)..."
  docker exec "$KAFKA_CLI_CONTAINER" kafka-console-consumer --bootstrap-server "$KAFKA_BOOTSTRAP" \
    --topic ridehailing.public.booking --from-beginning --max-messages 1 --timeout-ms 5000 >/dev/null 2>&1 \
    || info "Topic chưa có dữ liệu (có thể do generator chưa chạy)"
fi

# 9) (TUỲ CHỌN) CHẠY DATA GENERATOR
if [[ "${RUN_GENERATOR}" == "true" ]]; then
  info "Cài lib & chạy data generator..."
  python3 -m pip install --upgrade pip >/dev/null
  python3 -m pip install -q psycopg2-binary Faker >/dev/null
  CMD=(python3 generator/data_generator.py --mode "${GENERATOR_MODE}" --volume "${GENERATOR_VOLUME}" \
    --pg-host "${GENERATOR_PGHOST}" --pg-port "${GENERATOR_PGPORT}" \
    --pg-db "${GENERATOR_PGDB}" --pg-user "${GENERATOR_PGUSER}" \
    --pg-password "${GENERATOR_PGPASSWORD}")
  if [[ -n "${GENERATOR_EXTRA_ARGS}" ]]; then
    # shellcheck disable=SC2206
    EXTRA_ARGS=(${GENERATOR_EXTRA_ARGS})
    CMD+=("${EXTRA_ARGS[@]}")
  fi
  info "Generator cmd: ${CMD[*]}"
  if (( GENERATOR_SECONDS > 0 )); then
    info "Generator chạy ${GENERATOR_SECONDS}s..."
    timeout "${GENERATOR_SECONDS}" "${CMD[@]}" || true
  else
    info "Generator đang chạy (Ctrl+C để dừng ở cửa sổ này)..."
    "${CMD[@]}"
  fi
fi

# 10) KIỂM TRA NHANH ES & REPORTING
info "Kiểm tra nhanh Elasticsearch..."
curl -s -u "${ES_USERNAME}:${ES_PASSWORD}" "http://localhost:9200/${ES_INDEX}/_count" && echo
curl -s -u "${ES_USERNAME}:${ES_PASSWORD}" "http://localhost:9200/${ES_INDEX}/_search?size=1" | sed -n '1,120p' || true

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
