# Ride-hailing Streaming (Postgres → Debezium → Kafka → Flink → Postgres/ES)

## Run
### -1. Preflight (kiểm tra môi trường)

Luôn chạy bước này trước để tránh lỗi thiếu Docker daemon/CLI hoặc đụng port (Kafka/Elasticsearch/Postgres/Superset/Kibana/Flink):

```bash
./ops/preflight_check.sh
```

Script sẽ báo chi tiết và dừng sớm nếu phát hiện thiếu thành phần nào, sau đó bạn mới chuyển sang bước quick start ở dưới.

### 0. One-command quick start (full stack + generator + health checks)

Để chạy toàn bộ pipeline từ hạ tầng đến stream dữ liệu và kiểm tra health/SLA, dùng:

```bash
export ELASTIC_PASSWORD=changeme123   # nếu bạn bật security cho Elasticsearch/Kibana
RUN_GENERATOR=true GENERATOR_SECONDS=600 GENERATOR_VOLUME=small \
bash scripts/run_end_to_end.sh
```

Tuỳ chọn:

* `RUN_CORRECTNESS_TESTS=true` để chạy `tests/correctness_test.py` sau khi stack lên.
* `RUN_FAULT_TESTS=true` để chạy kịch bản resilience trong `tests/fault_tolerance_test.sh`.
* `RUN_PERF_TESTS=true` để chạy `tests/performance_test.py` (tốn thời gian hơn).

Script này gọi lại `scripts/redeploy_all.sh` (làm sạch volumes, dựng stack, deploy Flink, mapping Elasticsearch, đăng ký Debezium, bật generator theo cấu hình), rồi chạy `ops/check_health.sh` và `ops/check_sla_latency.sh`. Superset sẽ lên ở `http://localhost:8088`, Kibana ở `http://localhost:5601`.

### 1. Kịch bản "Full redeploy + stream" (khuyến nghị)

Script dưới đây gom toàn bộ các bước: hạ/khởi động lại Docker Compose, tạo mapping Elasticsearch, đăng ký Debezium, build & deploy Flink, sau đó bật generator chế độ **stream**. Chỉ cần chắc chắn Docker Desktop/daemon đã chạy trước khi thực thi.

> **Lưu ý:** Các script tiện ích trước đây (`workflow_stream_full.sh`, `workflow_stream_generator.sh`) đã được loại bỏ. Hãy sử dụng trực tiếp `scripts/redeploy_all.sh` hoặc các lệnh thủ công bên dưới.

```bash
# Mặc định generator chạy 10 phút (600 giây) ở chế độ stream, profile "small"
RUN_GENERATOR=true \
GENERATOR_SECONDS=600 \
GENERATOR_MODE=stream \
GENERATOR_VOLUME=small \
export ELASTIC_PASSWORD=changeme123
bash scripts/redeploy_all.sh
```

Các preset stream sẽ tự động scale thông số khi bạn đổi `GENERATOR_VOLUME`:

* `tiny/small` → tốc độ nhẹ để smoke test.
* `medium`     → tăng đồng thời số driver/passenger nền và lượng booking mới.
* `large`      → giảm thời gian ngủ xuống 50–150ms, giữ tối đa ~6.000 booking đang xử lý, phù hợp kiểm thử tải lớn (≈20× "small").

Khi cần canh chính xác hơn (ví dụ muốn tăng thêm 20 lần nữa), truyền `GENERATOR_EXTRA_ARGS` để ghi đè từng tham số `--stream-*`, ví dụ:

```bash
RUN_GENERATOR=true GENERATOR_VOLUME=large \
GENERATOR_EXTRA_ARGS="--stream-max-inflight 12000 --stream-new-booking-prob 0.995 --stream-sleep-min 0.02 --stream-sleep-max 0.06" \
bash scripts/redeploy_all.sh
```

> Cần chỉ định thông số kết nối Postgres khác? Sử dụng các biến môi trường
> `GENERATOR_PGHOST`, `GENERATOR_PGPORT`, `GENERATOR_PGDB`, `GENERATOR_PGUSER`
> và `GENERATOR_PGPASSWORD` trước khi chạy script (mặc định lần lượt là
> `localhost`, `5432`, `ride_hailing_db`, `user`, `password`).

> Script sẽ tự tạo Elasticsearch index `driver_locations`, template `driver_locations_timeseries-*` **và** data view (index pattern) tương ứng trong Kibana (`driver_locations` và `driver_locations_timeseries-*`). Sau khi chạy xong bạn có thể mở Kibana → *Stack Management → Data Views* để xác nhận đã có 2 data view sẵn sàng cho việc dựng map/dashboard.

> Hạ tầng mặc định chạy cụm Kafka 3 broker (`kafka-1`..`kafka-3`) kèm Schema Registry (mặc định bind ra host `18081`). Elasticsearch đã bật security (basic auth), user mặc định `elastic` với mật khẩu lấy từ biến môi trường `ELASTIC_PASSWORD` (mặc định `changeme123`); Kibana được cấu hình sẵn để kết nối bằng tài khoản này.

### 2. Kịch bản thủ công từng bước

```bash
# 1) Dựng hạ tầng cơ bản
docker compose up -d

# 2) Đăng ký lại Debezium connector
bash scripts/register_connector.sh

# 3) Build Flink job
cd flink-job && mvn -q -DskipTests package && cd ..

# 4) Submit job (JAR nằm trong /jars của JobManager)
docker exec -it flink-jobmanager flink run \
  -c com.ridehailing.RideHailingDataProcessor /jars/flink-job-1.0.0.jar

# 5) Bơm dữ liệu seed một lần (tuỳ chọn)
python3 generator/data_generator.py --mode seed --volume small

# 6) Phát sự kiện streaming liên tục (Ctrl+C để dừng)
# (ví dụ bên dưới mô phỏng preset "large" mặc định)
python3 generator/data_generator.py --mode stream \
  --volume large \
  --stream-max-inflight 6000 --stream-new-booking-prob 0.97 \
  --stream-driver-updates 28 --stream-sleep-min 0.05 --stream-sleep-max 0.15
```

### 3. Kịch bản "Chỉ chạy generator stream"

Sau khi hạ tầng + Flink job đã chạy ổn định, bạn có thể chỉ bật generator:

```bash
# Chạy 15 phút với cấu hình stream mặc định (volume "small")
timeout 900 python3 generator/data_generator.py --mode stream --volume small

# Hoặc giữ chạy vô hạn với cấu hình "large" được chỉnh mạnh tay hơn
python3 generator/data_generator.py --mode stream --volume large \
  --stream-max-inflight 12000 --stream-new-booking-prob 0.995 \
  --stream-driver-updates 32 --stream-sleep-min 0.02 --stream-sleep-max 0.06
```

### Kiểm chứng nhanh sau khi chạy

```bash
# Elasticsearch – driver latest + sample document
curl -s http://localhost:9200/driver_locations/_count
curl -s http://localhost:9200/driver_locations/_search?size=1 | jq .

# Postgres reporting – snapshot & fact minute mới nhất
psql -h localhost -p 5433 -U user -d reporting_db \
  -c "SELECT metric_name, metric_value, last_updated FROM mart.latest_metric_snapshot ORDER BY last_updated DESC LIMIT 10;"

psql -h localhost -p 5433 -U user -d reporting_db \
  -c "SELECT bucket_start, metric_name, metric_value FROM mart.fact_metric_bucket WHERE bucket_granularity = 'MINUTE' ORDER BY bucket_start DESC LIMIT 10;"
```

### Troubleshooting nhanh

* **Docker daemon chưa chạy** → `docker info` phải thành công trước khi gọi script. Nếu gặp lỗi “Cannot connect to the Docker daemon…”, hãy mở Docker Desktop rồi chạy lại.
* **Container `postgres-reporting` ở trạng thái Paused/Exited** → chạy `docker start postgres-reporting` (hoặc `docker unpause postgres-reporting`). Database được tạo mặc định là `reporting_db` trên cổng `5433`.
* **Muốn reprocess lịch sử** → đặt `FLINK_BOOKING_OFFSET_MODE=EARLIEST` (và tương tự cho các source khác) trước khi `redeploy_all.sh` để Flink đọc lại toàn bộ log CDC.
* **Điều chỉnh tốc độ generator** → sử dụng các tham số `--stream-*` (xem phần trợ giúp của script `generator/data_generator.py`).

## Còn thiếu gì để khớp 100% yêu cầu khóa luận

Các luồng realtime (Ops) và báo cáo (reporting_db) đã hoạt động đầy đủ cho bộ KPI Passenger/Driver/Operation, nhưng vẫn cần bổ sung vài hạng mục sau trước khi bàn giao cuối:

1. **Dashboard sẵn dùng**
   * Xuất bộ dashboard Kibana (*.ndjson) cho: overview realtime, heatmap supply/demand, driver online/busy, infra lag/backpressure.
   * Chuẩn bị template Power BI/Qlik (pbit/DAX/data model) để người dùng mở là có ngay 30+ KPI.

2. **Kịch bản generator đặc biệt**
   * Thêm chế độ surge/giờ cao điểm, thời tiết xấu (offline một phần tài xế), và thử nghiệm độ trễ mạng để stress-test latency và backpressure.

3. **Kiểm thử & benchmark**
   * Bộ script performance/resilience (pytest hoặc shell) cho 9 kịch bản: tăng tải 1k–5k eps, broker/taskmanager/DB restart, schema evolution, đo lag/latency p95/p99.

4. **Monitoring & alerting**
   * Thu thập metrics qua Prometheus/Grafana (Flink REST exporter, Kafka consumer lag, JVM/CPU/RAM) và rule cảnh báo: lag vượt ngưỡng, match/cancel rate tụt, 5xx tăng.

5. **Bảo mật & HA Kafka**
   * Bật X-Pack auth cho Elasticsearch/Kibana; thêm Schema Registry và cụm Kafka 3 broker (replication ≥3, topic.creation.* tương ứng); cấu hình TLS/RBAC nếu triển khai thực tế.

6. **Cohort/Retention batch**
   * Job/batch hàng ngày tính cohort rider/driver, retention/churn/return theo t-1/t-3 và productivity Trips/Driver/Day để phục vụ dashboard NRT.

7. **Data quality & backfill**
   * Chính sách reprocess/backfill khi schema đổi, theo dõi completeness/duplication rate, và cảnh báo freshness (lag) trực tiếp trên dashboard.

Các mục trên không chặn luồng realtime hiện tại nhưng cần hoàn thiện để đáp ứng đủ phạm vi và tiêu chí đánh giá của khóa luận.
