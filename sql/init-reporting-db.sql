-- Khởi tạo database reporting cho bài toán ride-hailing
-- Bao gồm schema mart, bảng fact, bảng KPI và các view phục vụ BI

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Dọn sạch schema mart để đảm bảo script có thể chạy nhiều lần (dev/test)
DROP SCHEMA IF EXISTS mart CASCADE;
CREATE SCHEMA mart;

-- =====================================================================
-- Từ điển metric mô tả rõ ý nghĩa, cách cộng gộp và đơn vị đo
-- =====================================================================
DROP TABLE IF EXISTS mart.metric_catalog CASCADE;
CREATE TABLE mart.metric_catalog (
    metric_name          TEXT PRIMARY KEY,
    display_name         TEXT        NOT NULL,
    subject_area         TEXT        NOT NULL,
    metric_group         TEXT        NOT NULL,
    value_type           TEXT        NOT NULL,
    default_unit         TEXT        NOT NULL,
    description          TEXT,
    aggregation_method   TEXT        NOT NULL DEFAULT 'SUM',
    is_additive          BOOLEAN     NOT NULL DEFAULT TRUE,
    lower_is_better      BOOLEAN     NOT NULL DEFAULT FALSE,
    is_realtime          BOOLEAN     NOT NULL DEFAULT TRUE,
    default_granularity  TEXT        NOT NULL DEFAULT 'MINUTE',
    numerator_metric     TEXT,
    denominator_metric   TEXT,
    sample_metric        TEXT,
    notes                TEXT,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ck_metric_catalog_value_type CHECK (
        value_type IN (
            'COUNT', 'CURRENCY', 'PERCENT', 'RATIO', 'DURATION', 'INTEGER', 'FLOAT'
        )
    ),
    CONSTRAINT ck_metric_catalog_agg_method CHECK (
        aggregation_method IN ('SUM', 'WEIGHTED_AVG', 'LATEST')
    ),
    CONSTRAINT ck_metric_catalog_granularity CHECK (
        default_granularity IN ('MINUTE', 'HOUR', 'DAY', 'WEEK', 'MONTH')
    )
);

INSERT INTO mart.metric_catalog (
    metric_name,
    display_name,
    subject_area,
    metric_group,
    value_type,
    default_unit,
    description,
    aggregation_method,
    is_additive,
    lower_is_better,
    is_realtime,
    default_granularity,
    numerator_metric,
    denominator_metric,
    sample_metric,
    notes
)
VALUES
    -- Passenger metrics
    ('new_passenger',          'New Passenger',           'Passenger', 'Acquisition',   'COUNT',    'COUNT',   'Số lượng hành khách đăng ký mới và chưa đặt chuyến.',                                 'SUM',          TRUE,  FALSE, FALSE, 'DAY',   NULL, NULL, NULL, NULL),
    ('active_passenger',       'Active Passenger',        'Passenger', 'Engagement',    'COUNT',    'COUNT',   'Số lượng hành khách hoàn thành ít nhất một chuyến trong kỳ.',                       'SUM',          TRUE,  FALSE, FALSE, 'DAY',   NULL, NULL, NULL, NULL),
    ('retention_passenger',    'Retention Passenger',     'Passenger', 'Retention',     'COUNT',    'COUNT',   'Số lượng hành khách tiếp tục hoạt động từ kỳ t-1 sang t.',                         'SUM',          TRUE,  FALSE, FALSE, 'DAY',   NULL, NULL, NULL, 'Cần dữ liệu cohort t-1.'),
    ('retention_rate_passenger','Retention Rate',         'Passenger', 'Retention',     'PERCENT',  'PERCENT', 'Tỷ lệ khách hàng còn hoạt động từ kỳ t-1 sang t.',                                  'WEIGHTED_AVG', FALSE, FALSE, FALSE, 'DAY',   'retention_passenger', 'active_passenger_prev', NULL, NULL),
    ('churn_passenger',        'Churn Passenger',         'Passenger', 'Retention',     'COUNT',    'COUNT',   'Số lượng hành khách dừng hoạt động từ kỳ t-3 sang t.',                             'SUM',          TRUE,  TRUE,  FALSE, 'DAY',   NULL, NULL, NULL, NULL),
    ('return_passenger',       'Return Passenger',        'Passenger', 'Retention',     'COUNT',    'COUNT',   'Số lượng hành khách đã churn nhưng quay trở lại sử dụng dịch vụ.',                 'SUM',          TRUE,  FALSE, FALSE, 'DAY',   NULL, NULL, NULL, NULL),
    ('churn_rate_passenger',   'Churn Rate',              'Passenger', 'Retention',     'PERCENT',  'PERCENT', 'Tỷ lệ khách hàng churn trong kỳ t so với t-3.',                                    'WEIGHTED_AVG', FALSE, TRUE,  FALSE, 'DAY',   'churn_passenger', 'active_passenger_tminus3', NULL, NULL),

    -- Driver metrics
    ('new_driver',             'New Driver',              'Driver',    'Acquisition',   'COUNT',    'COUNT',   'Số lượng tài xế đăng ký mới và chưa nhận chuyến.',                                 'SUM',          TRUE,  FALSE, FALSE, 'DAY',   NULL, NULL, NULL, NULL),
    ('active_driver',          'Active Driver',           'Driver',    'Engagement',    'COUNT',    'COUNT',   'Số lượng tài xế hoàn thành ít nhất một chuyến trong kỳ.',                         'SUM',          TRUE,  FALSE, FALSE, 'DAY',   NULL, NULL, NULL, NULL),
    ('online_driver',          'Online Driver',           'Driver',    'Availability',  'COUNT',    'COUNT',   'Số lượng tài xế bật trạng thái trực tuyến.',                                      'SUM',          TRUE,  FALSE, TRUE,  'MINUTE',NULL, NULL, NULL, 'Có thể tính trung bình hoặc thời điểm cuối.'),
    ('busy_driver',            'Busy Driver',             'Driver',    'Availability',  'COUNT',    'COUNT',   'Số lượng tài xế đang thực hiện chuyến xe.',                                      'SUM',          TRUE,  FALSE, TRUE,  'MINUTE',NULL, NULL, NULL, NULL),
    ('retention_driver',       'Retention Driver',        'Driver',    'Retention',     'COUNT',    'COUNT',   'Số lượng tài xế tiếp tục hoạt động từ kỳ t-1 sang t.',                            'SUM',          TRUE,  FALSE, FALSE, 'DAY',   NULL, NULL, NULL, 'Cần cohort driver.'),
    ('retention_rate_driver',  'Driver Retention Rate',   'Driver',    'Retention',     'PERCENT',  'PERCENT', 'Tỷ lệ tài xế còn hoạt động từ kỳ t-1 sang t.',                                   'WEIGHTED_AVG', FALSE, FALSE, FALSE, 'DAY',   'retention_driver', 'active_driver_prev', NULL, NULL),
    ('churn_driver',           'Churn Driver',            'Driver',    'Retention',     'COUNT',    'COUNT',   'Số lượng tài xế ngưng hoạt động từ kỳ t-3 sang t.',                              'SUM',          TRUE,  TRUE,  FALSE, 'DAY',   NULL, NULL, NULL, NULL),
    ('churn_rate_driver',      'Driver Churn Rate',       'Driver',    'Retention',     'PERCENT',  'PERCENT', 'Tỷ lệ tài xế ngưng hoạt động từ kỳ t-3 sang t.',                                 'WEIGHTED_AVG', FALSE, TRUE,  FALSE, 'DAY',   'churn_driver', 'active_driver_tminus3', NULL, NULL),
    ('return_driver',          'Return Driver',           'Driver',    'Retention',     'COUNT',    'COUNT',   'Số lượng tài xế đã churn nhưng quay lại.',                                     'SUM',          TRUE,  FALSE, FALSE, 'DAY',   NULL, NULL, NULL, NULL),
    ('productivity',           'Productivity',            'Driver',    'Efficiency',    'RATIO',    'TRIP_PER_DRIVER','Số chuyến hoàn thành trên mỗi tài xế hoạt động.',                           'WEIGHTED_AVG', FALSE, FALSE, FALSE, 'DAY',   'completed_order', 'active_driver', NULL, 'Có thể tính theo ngày/tuần/tháng.'),
    ('net_driver_income',      'Net Driver Income',       'Driver',    'Finance',       'CURRENCY', 'CURRENCY','Thu nhập ròng tài xế nhận được (sau chia sẻ).',                                'SUM',          TRUE,  FALSE, TRUE,  'MINUTE',NULL, NULL, NULL, NULL),

    -- Operations metrics
    ('request_order',          'Request Order',           'Operation', 'Demand',        'COUNT',    'COUNT',   'Tổng số chuyến xe được tạo trên hệ thống.',                                     'SUM',          TRUE,  FALSE, TRUE,  'MINUTE',NULL, NULL, NULL, NULL),
    ('accept_order',           'Accept Order',            'Operation', 'Fulfillment',   'COUNT',    'COUNT',   'Tổng số chuyến xe được tài xế chấp nhận.',                                     'SUM',          TRUE,  FALSE, TRUE,  'MINUTE',NULL, NULL, NULL, NULL),
    ('acceptance_rate',        'Acceptance Rate',         'Operation', 'Fulfillment',   'PERCENT',  'PERCENT', 'Tỷ lệ chuyến xe được chấp nhận.',                                                'WEIGHTED_AVG', FALSE, FALSE, TRUE,  'MINUTE','accept_order', 'request_order', NULL, NULL),
    ('completed_order',        'Completed Order',         'Operation', 'Fulfillment',   'COUNT',    'COUNT',   'Tổng số chuyến xe hoàn thành.',                                                'SUM',          TRUE,  FALSE, TRUE,  'MINUTE',NULL, NULL, NULL, NULL),
    ('completed_rate',         'Completed Rate',          'Operation', 'Fulfillment',   'PERCENT',  'PERCENT', 'Tỷ lệ chuyến xe hoàn thành.',                                                   'WEIGHTED_AVG', FALSE, FALSE, TRUE,  'MINUTE','completed_order', 'request_order', NULL, NULL),
    ('gsv',                    'Gross Service Value',     'Operation', 'Finance',       'CURRENCY', 'CURRENCY','Tổng tiền thu từ khách hàng.',                                                'SUM',          TRUE,  FALSE, TRUE,  'MINUTE',NULL, NULL, NULL, NULL),
    ('revenue_vat',            'Revenue include VAT',     'Operation', 'Finance',       'CURRENCY', 'CURRENCY','Doanh thu bao gồm VAT.',                                                        'SUM',          TRUE,  FALSE, TRUE,  'MINUTE',NULL, NULL, NULL, NULL),
    ('revenue_net',            'Revenue',                 'Operation', 'Finance',       'CURRENCY', 'CURRENCY','Doanh thu sau VAT.',                                                           'SUM',          TRUE,  FALSE, TRUE,  'MINUTE',NULL, NULL, NULL, NULL),
    ('cancel_order',           'Cancel Order',            'Operation', 'Fulfillment',   'COUNT',    'COUNT',   'Tổng số chuyến xe bị hủy.',                                                     'SUM',          TRUE,  TRUE,  TRUE,  'MINUTE',NULL, NULL, NULL, NULL),
    ('cancel_rate',            'Cancellation Rate',       'Operation', 'Fulfillment',   'PERCENT',  'PERCENT', 'Tỷ lệ chuyến xe bị hủy.',                                                       'WEIGHTED_AVG', FALSE, TRUE,  TRUE,  'MINUTE','cancel_order', 'request_order', NULL, NULL),
    ('order_time',             'Order Time',              'Operation', 'Time',          'DURATION', 'SECOND',  'Khoảng thời gian từ create_time đến accept_time.',                              'WEIGHTED_AVG', FALSE, TRUE,  TRUE,  'MINUTE','order_time_total', 'completed_order', NULL, NULL),
    ('board_time',             'Board Time',              'Operation', 'Time',          'DURATION', 'SECOND',  'Khoảng thời gian từ lúc khách lên xe đến hoàn thành.',                          'WEIGHTED_AVG', FALSE, TRUE,  TRUE,  'MINUTE','board_time_total', 'completed_order', NULL, NULL),
    ('accept_time',            'Accept Time',             'Operation', 'Time',          'DURATION', 'SECOND',  'Khoảng thời gian từ yêu cầu đến khi tài xế chấp nhận.',                        'WEIGHTED_AVG', FALSE, TRUE,  TRUE,  'MINUTE','accept_time_total', 'request_order', NULL, NULL),
    ('lead_time',              'Lead Time',               'Operation', 'Time',          'DURATION', 'SECOND',  'Thời gian từ khi tạo chuyến đến khi hoàn thành.',                               'WEIGHTED_AVG', FALSE, TRUE,  TRUE,  'MINUTE','lead_time_total', 'completed_order', NULL, NULL),
    ('discount_order',         'Discount Order',          'Operation', 'Finance',       'CURRENCY', 'CURRENCY','Tổng giá trị khuyến mãi áp dụng cho chuyến.',                                  'SUM',          TRUE,  TRUE,  TRUE,  'MINUTE',NULL, NULL, NULL, NULL),

    -- Derived supporting metrics (không hiển thị trực tiếp)
    ('order_time_total',       'Order Time Total',        'Derived',   'Support',       'DURATION', 'SECOND',  'Tổng thời gian tích lũy tính Order Time.',                                      'SUM',          TRUE,  TRUE,  TRUE,  'MINUTE',NULL, NULL, NULL, 'Không lộ ra dashboard.'),
    ('board_time_total',       'Board Time Total',        'Derived',   'Support',       'DURATION', 'SECOND',  'Tổng thời gian tích lũy tính Board Time.',                                      'SUM',          TRUE,  TRUE,  TRUE,  'MINUTE',NULL, NULL, NULL, 'Không lộ ra dashboard.'),
    ('accept_time_total',      'Accept Time Total',       'Derived',   'Support',       'DURATION', 'SECOND',  'Tổng thời gian tích lũy tính Accept Time.',                                     'SUM',          TRUE,  TRUE,  TRUE,  'MINUTE',NULL, NULL, NULL, 'Không lộ ra dashboard.'),
    ('lead_time_total',        'Lead Time Total',         'Derived',   'Support',       'DURATION', 'SECOND',  'Tổng thời gian tích lũy tính Lead Time.',                                       'SUM',          TRUE,  TRUE,  TRUE,  'MINUTE',NULL, NULL, NULL, 'Không lộ ra dashboard.'),
    ('active_passenger_prev',  'Active Passenger t-1',    'Derived',   'Support',       'COUNT',    'COUNT',   'Số active passenger của kỳ trước (denominator retention).',                    'SUM',          TRUE,  FALSE, FALSE, 'DAY',   NULL, NULL, NULL, 'Được Flink populate.'),
    ('active_passenger_tminus3','Active Passenger t-3',   'Derived',   'Support',       'COUNT',    'COUNT',   'Số active passenger tại kỳ t-3.',                                             'SUM',          TRUE,  FALSE, FALSE, 'DAY',   NULL, NULL, NULL, NULL),
    ('active_driver_prev',     'Active Driver t-1',       'Derived',   'Support',       'COUNT',    'COUNT',   'Số active driver kỳ trước.',                                                    'SUM',          TRUE,  FALSE, FALSE, 'DAY',   NULL, NULL, NULL, NULL),
    ('active_driver_tminus3',  'Active Driver t-3',       'Derived',   'Support',       'COUNT',    'COUNT',   'Số active driver tại kỳ t-3.',                                                 'SUM',          TRUE,  FALSE, FALSE, 'DAY',   NULL, NULL, NULL, NULL)
ON CONFLICT (metric_name) DO UPDATE
SET display_name        = EXCLUDED.display_name,
    subject_area        = EXCLUDED.subject_area,
    metric_group        = EXCLUDED.metric_group,
    value_type          = EXCLUDED.value_type,
    default_unit        = EXCLUDED.default_unit,
    description         = EXCLUDED.description,
    aggregation_method  = EXCLUDED.aggregation_method,
    is_additive         = EXCLUDED.is_additive,
    lower_is_better     = EXCLUDED.lower_is_better,
    is_realtime         = EXCLUDED.is_realtime,
    default_granularity = EXCLUDED.default_granularity,
    numerator_metric    = EXCLUDED.numerator_metric,
    denominator_metric  = EXCLUDED.denominator_metric,
    sample_metric       = EXCLUDED.sample_metric,
    notes               = EXCLUDED.notes;

-- =====================================================================
-- Bảng fact chi tiết cho từng bucket thời gian, scope và metric
-- =====================================================================
DROP TABLE IF EXISTS mart.fact_metric_bucket CASCADE;
CREATE TABLE mart.fact_metric_bucket (
    bucket_start         TIMESTAMPTZ NOT NULL,
    bucket_granularity   TEXT        NOT NULL DEFAULT 'MINUTE',
    metric_name          TEXT        NOT NULL,
    service_type         TEXT        NOT NULL DEFAULT '',
    service_tier         TEXT        NOT NULL DEFAULT '',
    area_code            TEXT        NOT NULL DEFAULT '',
    cohort_key           TEXT        NOT NULL DEFAULT 'ALL',
    entity_type          TEXT        NOT NULL DEFAULT 'AGGREGATE',
    entity_id            TEXT        NOT NULL DEFAULT 'ALL',
    metric_value         NUMERIC(20,6) NOT NULL DEFAULT 0,
    numerator_value      NUMERIC(20,6) NOT NULL DEFAULT 0,
    denominator_value    NUMERIC(20,6) NOT NULL DEFAULT 0,
    sample_size          NUMERIC(20,6) NOT NULL DEFAULT 0,
    first_event_at       TIMESTAMPTZ,
    last_event_at        TIMESTAMPTZ,
    last_ingested_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    processing_lag_ms    INTEGER      NOT NULL DEFAULT 0,
    last_updated         TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (
        metric_name,
        bucket_start,
        bucket_granularity,
        service_type,
        service_tier,
        area_code,
        cohort_key,
        entity_type,
        entity_id
    ),
    CONSTRAINT fk_fact_metric_catalog FOREIGN KEY (metric_name)
        REFERENCES mart.metric_catalog(metric_name),
    CONSTRAINT ck_fact_metric_bucket_granularity CHECK (
        bucket_granularity IN ('MINUTE', 'HOUR', 'DAY', 'WEEK', 'MONTH')
    ),
    CONSTRAINT ck_fact_metric_bucket_non_negative CHECK (
        metric_value      >= 0 AND
        numerator_value   >= 0 AND
        denominator_value >= 0 AND
        sample_size       >= 0 AND
        processing_lag_ms >= 0
    )
);

COMMENT ON TABLE mart.fact_metric_bucket IS 'Chi tiết KPI theo từng metric, bucket thời gian và scope (service/cohort/entity) dùng cho dashboard.';
COMMENT ON COLUMN mart.fact_metric_bucket.cohort_key IS 'Khóa biểu diễn cohort (ví dụ tháng đăng ký, vùng hoạt động).';
COMMENT ON COLUMN mart.fact_metric_bucket.entity_type IS 'Loại thực thể (AGGREGATE, DRIVER, PASSENGER, ORDER, ...).';
COMMENT ON COLUMN mart.fact_metric_bucket.sample_size IS 'Sử dụng khi cần trọng số phụ ngoài denominator (ví dụ số mẫu khảo sát).';

CREATE INDEX IF NOT EXISTS ix_fact_metric_bucket_time_desc
    ON mart.fact_metric_bucket (bucket_start DESC)
    INCLUDE (metric_name, metric_value, last_ingested_at);
CREATE INDEX IF NOT EXISTS ix_fact_metric_bucket_scope
    ON mart.fact_metric_bucket (service_type, service_tier, area_code, cohort_key);
CREATE INDEX IF NOT EXISTS ix_fact_metric_bucket_metric
    ON mart.fact_metric_bucket (metric_name);
CREATE INDEX IF NOT EXISTS ix_fact_metric_bucket_entity
    ON mart.fact_metric_bucket (entity_type, entity_id);
CREATE INDEX IF NOT EXISTS ix_fact_metric_bucket_last_event
    ON mart.fact_metric_bucket (last_event_at DESC);

-- =====================================================================
-- Bảng KPI realtime (snapshot mới nhất theo metric & scope)
-- =====================================================================
DROP TABLE IF EXISTS mart.latest_metric_snapshot CASCADE;
CREATE TABLE mart.latest_metric_snapshot (
    metric_name        TEXT        NOT NULL,
    service_type       TEXT        NOT NULL DEFAULT '',
    service_tier       TEXT        NOT NULL DEFAULT '',
    area_code          TEXT        NOT NULL DEFAULT '',
    cohort_key         TEXT        NOT NULL DEFAULT 'ALL',
    entity_type        TEXT        NOT NULL DEFAULT 'AGGREGATE',
    entity_id          TEXT        NOT NULL DEFAULT 'ALL',
    metric_value       NUMERIC(20,6) NOT NULL DEFAULT 0,
    numerator_value    NUMERIC(20,6) NOT NULL DEFAULT 0,
    denominator_value  NUMERIC(20,6) NOT NULL DEFAULT 0,
    sample_size        NUMERIC(20,6) NOT NULL DEFAULT 0,
    metric_unit        TEXT         NOT NULL DEFAULT 'COUNT',
    data_granularity   TEXT         NOT NULL DEFAULT 'MINUTE',
    event_time         TIMESTAMPTZ  NOT NULL DEFAULT now(),
    last_ingested_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    last_updated       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT pk_latest_metric_snapshot PRIMARY KEY (
        metric_name,
        service_type,
        service_tier,
        area_code,
        cohort_key,
        entity_type,
        entity_id
    ),
    CONSTRAINT fk_latest_metric_catalog FOREIGN KEY (metric_name)
        REFERENCES mart.metric_catalog(metric_name)
);

CREATE INDEX IF NOT EXISTS ix_latest_metric_name
    ON mart.latest_metric_snapshot (metric_name);
CREATE INDEX IF NOT EXISTS ix_latest_metric_scope_updated
    ON mart.latest_metric_snapshot (last_updated DESC);

-- =====================================================================
-- Bảng lưu trữ vị trí tài xế và lộ trình chuyến đi cho nhu cầu BI
-- =====================================================================
DROP TABLE IF EXISTS mart.driver_location_timeseries;
CREATE TABLE mart.driver_location_timeseries (
    driver_id        TEXT        NOT NULL,
    service_type     TEXT        NOT NULL DEFAULT '',
    service_tier     TEXT        NOT NULL DEFAULT '',
    area_code        TEXT        NOT NULL DEFAULT '',
    latitude         NUMERIC(10,6) NOT NULL,
    longitude        NUMERIC(10,6) NOT NULL,
    heading_degree   NUMERIC(6,2),
    speed_kmh        NUMERIC(10,2),
    driver_status    TEXT        NOT NULL DEFAULT '',
    event_time       TIMESTAMPTZ NOT NULL,
    received_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (driver_id, event_time)
);

CREATE INDEX IF NOT EXISTS ix_driver_location_time
    ON mart.driver_location_timeseries (event_time DESC);
CREATE INDEX IF NOT EXISTS ix_driver_location_area
    ON mart.driver_location_timeseries (area_code, service_type);

DROP TABLE IF EXISTS mart.booking_route_segments;
CREATE TABLE mart.booking_route_segments (
    booking_id     TEXT        NOT NULL,
    segment_type   TEXT        NOT NULL DEFAULT 'PATH',
    sequence_no    INTEGER     NOT NULL,
    latitude       NUMERIC(10,6) NOT NULL,
    longitude      NUMERIC(10,6) NOT NULL,
    event_time     TIMESTAMPTZ,
    received_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (booking_id, segment_type, sequence_no)
);

CREATE INDEX IF NOT EXISTS ix_booking_route_time
    ON mart.booking_route_segments (event_time DESC);

-- =====================================================================
-- Các view phục vụ báo cáo/Power BI
-- =====================================================================
DROP VIEW IF EXISTS mart.vw_metric_minute;
CREATE VIEW mart.vw_metric_minute AS
SELECT
    f.bucket_start,
    f.service_type,
    f.service_tier,
    f.area_code,
    f.cohort_key,
    f.entity_type,
    f.entity_id,
    f.metric_name,
    mc.display_name,
    mc.subject_area,
    mc.metric_group,
    mc.value_type,
    mc.default_unit,
    mc.aggregation_method,
    mc.lower_is_better,
    CASE
        WHEN mc.is_additive THEN f.metric_value
        WHEN f.denominator_value > 0 THEN f.numerator_value / f.denominator_value
        WHEN f.sample_size > 0 THEN f.numerator_value / f.sample_size
        ELSE NULL
    END AS metric_value,
    f.numerator_value,
    f.denominator_value,
    f.sample_size,
    f.first_event_at,
    f.last_event_at,
    f.last_ingested_at,
    f.processing_lag_ms,
    f.last_updated
FROM mart.fact_metric_bucket f
JOIN mart.metric_catalog mc ON mc.metric_name = f.metric_name
WHERE f.bucket_granularity = 'MINUTE'
  AND mc.subject_area <> 'Derived';

DROP VIEW IF EXISTS mart.vw_metric_hourly;
CREATE VIEW mart.vw_metric_hourly AS
SELECT
    f.bucket_start     AS bucket_hour,
    f.service_type,
    f.service_tier,
    f.area_code,
    f.cohort_key,
    f.entity_type,
    f.entity_id,
    f.metric_name,
    mc.display_name,
    mc.subject_area,
    mc.metric_group,
    mc.value_type,
    mc.default_unit,
    mc.lower_is_better,
    CASE
        WHEN mc.is_additive THEN f.metric_value
        WHEN f.denominator_value > 0 THEN f.numerator_value / f.denominator_value
        WHEN f.sample_size > 0 THEN f.numerator_value / f.sample_size
        ELSE NULL
    END AS metric_value,
    f.numerator_value,
    f.denominator_value,
    f.sample_size,
    f.first_event_at,
    f.last_event_at,
    f.last_ingested_at,
    f.processing_lag_ms,
    f.last_updated
FROM mart.fact_metric_bucket f
JOIN mart.metric_catalog mc ON mc.metric_name = f.metric_name
WHERE f.bucket_granularity = 'HOUR'
  AND mc.subject_area <> 'Derived';

DROP VIEW IF EXISTS mart.vw_metric_daily;
CREATE VIEW mart.vw_metric_daily AS
SELECT
    f.bucket_start     AS bucket_day,
    f.service_type,
    f.service_tier,
    f.area_code,
    f.cohort_key,
    f.entity_type,
    f.entity_id,
    f.metric_name,
    mc.display_name,
    mc.subject_area,
    mc.metric_group,
    mc.value_type,
    mc.default_unit,
    mc.lower_is_better,
    CASE
        WHEN mc.is_additive THEN f.metric_value
        WHEN f.denominator_value > 0 THEN f.numerator_value / f.denominator_value
        WHEN f.sample_size > 0 THEN f.numerator_value / f.sample_size
        ELSE NULL
    END AS metric_value,
    f.numerator_value,
    f.denominator_value,
    f.sample_size,
    f.first_event_at,
    f.last_event_at,
    f.last_ingested_at,
    f.processing_lag_ms,
    f.last_updated
FROM mart.fact_metric_bucket f
JOIN mart.metric_catalog mc ON mc.metric_name = f.metric_name
WHERE f.bucket_granularity = 'DAY'
  AND mc.subject_area <> 'Derived';

DROP VIEW IF EXISTS mart.vw_metric_weekly;
CREATE VIEW mart.vw_metric_weekly AS
SELECT
    f.bucket_start     AS bucket_week,
    f.service_type,
    f.service_tier,
    f.area_code,
    f.cohort_key,
    f.entity_type,
    f.entity_id,
    f.metric_name,
    mc.display_name,
    mc.subject_area,
    mc.metric_group,
    mc.value_type,
    mc.default_unit,
    mc.lower_is_better,
    CASE
        WHEN mc.is_additive THEN f.metric_value
        WHEN f.denominator_value > 0 THEN f.numerator_value / f.denominator_value
        WHEN f.sample_size > 0 THEN f.numerator_value / f.sample_size
        ELSE NULL
    END AS metric_value,
    f.numerator_value,
    f.denominator_value,
    f.sample_size,
    f.first_event_at,
    f.last_event_at,
    f.last_ingested_at,
    f.processing_lag_ms,
    f.last_updated
FROM mart.fact_metric_bucket f
JOIN mart.metric_catalog mc ON mc.metric_name = f.metric_name
WHERE f.bucket_granularity = 'WEEK'
  AND mc.subject_area <> 'Derived';

DROP VIEW IF EXISTS mart.vw_metric_monthly;
CREATE VIEW mart.vw_metric_monthly AS
SELECT
    f.bucket_start     AS bucket_month,
    f.service_type,
    f.service_tier,
    f.area_code,
    f.cohort_key,
    f.entity_type,
    f.entity_id,
    f.metric_name,
    mc.display_name,
    mc.subject_area,
    mc.metric_group,
    mc.value_type,
    mc.default_unit,
    mc.lower_is_better,
    CASE
        WHEN mc.is_additive THEN f.metric_value
        WHEN f.denominator_value > 0 THEN f.numerator_value / f.denominator_value
        WHEN f.sample_size > 0 THEN f.numerator_value / f.sample_size
        ELSE NULL
    END AS metric_value,
    f.numerator_value,
    f.denominator_value,
    f.sample_size,
    f.first_event_at,
    f.last_event_at,
    f.last_ingested_at,
    f.processing_lag_ms,
    f.last_updated
FROM mart.fact_metric_bucket f
JOIN mart.metric_catalog mc ON mc.metric_name = f.metric_name
WHERE f.bucket_granularity = 'MONTH'
  AND mc.subject_area <> 'Derived';

DROP VIEW IF EXISTS mart.vw_metric_scope_snapshot;
CREATE VIEW mart.vw_metric_scope_snapshot AS
SELECT
    f.service_type,
    f.service_tier,
    f.area_code,
    f.cohort_key,
    f.entity_type,
    f.entity_id,
    f.metric_name,
    mc.display_name,
    mc.subject_area,
    mc.metric_group,
    mc.value_type,
    mc.default_unit,
    mc.lower_is_better,
    CASE
        WHEN mc.is_additive THEN SUM(f.metric_value)
        WHEN SUM(f.denominator_value) > 0 THEN SUM(f.numerator_value) / SUM(f.denominator_value)
        WHEN SUM(f.sample_size) > 0 THEN SUM(f.numerator_value) / SUM(f.sample_size)
        ELSE NULL
    END AS metric_value,
    SUM(f.numerator_value)   AS numerator_value,
    SUM(f.denominator_value) AS denominator_value,
    SUM(f.sample_size)       AS sample_size,
    MAX(f.last_event_at)     AS last_event_at,
    MAX(f.last_ingested_at)  AS last_ingested_at
FROM mart.fact_metric_bucket f
JOIN mart.metric_catalog mc ON mc.metric_name = f.metric_name
WHERE mc.subject_area <> 'Derived'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13;

DROP VIEW IF EXISTS public.realtime_kpi_metrics;
CREATE VIEW public.realtime_kpi_metrics AS
SELECT
    k.metric_name,
    mc.display_name,
    mc.subject_area,
    mc.metric_group,
    mc.value_type,
    COALESCE(k.metric_unit, mc.default_unit) AS unit,
    mc.description,
    mc.aggregation_method,
    mc.lower_is_better,
    mc.is_realtime,
    k.data_granularity,
    k.service_type,
    k.service_tier,
    k.area_code,
    k.cohort_key,
    k.entity_type,
    k.entity_id,
    CASE
        WHEN mc.is_additive THEN k.metric_value
        WHEN k.denominator_value > 0 THEN k.numerator_value / k.denominator_value
        WHEN k.sample_size > 0 THEN k.numerator_value / k.sample_size
        ELSE NULL
    END AS metric_value,
    k.numerator_value,
    k.denominator_value,
    k.sample_size,
    k.event_time,
    k.last_ingested_at,
    k.last_updated
FROM mart.latest_metric_snapshot k
LEFT JOIN mart.metric_catalog mc ON mc.metric_name = k.metric_name
WHERE mc.subject_area <> 'Derived';

DROP VIEW IF EXISTS mart.vw_latest_driver_location;
CREATE VIEW mart.vw_latest_driver_location AS
SELECT driver_id,
       service_type,
       service_tier,
       area_code,
       latitude,
       longitude,
       heading_degree,
       speed_kmh,
       driver_status,
       event_time,
       received_at
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY driver_id ORDER BY event_time DESC) AS rn
    FROM mart.driver_location_timeseries
) s
WHERE rn = 1;

DROP VIEW IF EXISTS mart.vw_booking_route_paths;
CREATE VIEW mart.vw_booking_route_paths AS
SELECT
    booking_id,
    segment_type,
    ARRAY_AGG(jsonb_build_object(
        'sequence_no', sequence_no,
        'lat', latitude,
        'lng', longitude,
        'event_time', event_time
    ) ORDER BY sequence_no) AS path_points,
    MIN(event_time) AS started_at,
    MAX(event_time) AS ended_at,
    MAX(received_at) AS last_received_at
FROM mart.booking_route_segments
GROUP BY booking_id, segment_type;

DROP VIEW IF EXISTS mart.vw_data_freshness;
CREATE VIEW mart.vw_data_freshness AS
SELECT
    'fact_metric_bucket' AS source_name,
    MAX(last_event_at)    AS last_event_time,
    MAX(last_ingested_at) AS last_ingested_at,
    EXTRACT(EPOCH FROM (now() - MAX(last_ingested_at)))::INT AS ingestion_lag_seconds,
    EXTRACT(EPOCH FROM (MAX(last_ingested_at) - MAX(last_event_at)))::INT AS processing_lag_seconds
FROM mart.fact_metric_bucket
UNION ALL
SELECT
    'latest_metric_snapshot' AS source_name,
    MAX(event_time),
    MAX(last_ingested_at),
    EXTRACT(EPOCH FROM (now() - MAX(last_ingested_at)))::INT,
    EXTRACT(EPOCH FROM (MAX(last_ingested_at) - MAX(event_time)))::INT
FROM mart.latest_metric_snapshot
UNION ALL
SELECT
    'driver_location_timeseries' AS source_name,
    MAX(event_time),
    MAX(received_at),
    EXTRACT(EPOCH FROM (now() - MAX(received_at)))::INT,
    EXTRACT(EPOCH FROM (MAX(received_at) - MAX(event_time)))::INT
FROM mart.driver_location_timeseries
UNION ALL
SELECT
    'booking_route_segments' AS source_name,
    MAX(event_time),
    MAX(received_at),
    EXTRACT(EPOCH FROM (now() - MAX(received_at)))::INT,
    EXTRACT(EPOCH FROM (MAX(received_at) - MAX(event_time)))::INT
FROM mart.booking_route_segments;

-- Phân quyền đọc cho BI/analyst (tùy chọn)
-- GRANT USAGE ON SCHEMA mart TO analyst;