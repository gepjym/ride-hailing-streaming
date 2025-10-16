-- Khởi tạo DB reporting: schema & bảng mart
CREATE SCHEMA IF NOT EXISTS mart;

-- =========================
-- FACT THEO PHÚT (event-time)
-- =========================
DROP TABLE IF EXISTS mart.fact_trip_minute CASCADE;
CREATE TABLE mart.fact_trip_minute (
    bucket_start           TIMESTAMPTZ NOT NULL,
    service_type           TEXT        NOT NULL DEFAULT '',
    service_tier           TEXT        NOT NULL DEFAULT '',
    area_code              TEXT        NOT NULL DEFAULT '',
    requests_total         BIGINT      NOT NULL DEFAULT 0,
    accepted_total         BIGINT      NOT NULL DEFAULT 0,
    completed_total        BIGINT      NOT NULL DEFAULT 0,
    canceled_total         BIGINT      NOT NULL DEFAULT 0,
    gmv_total              NUMERIC(18,2) NOT NULL DEFAULT 0,
    revenue_total          NUMERIC(18,2) NOT NULL DEFAULT 0,
    discount_amount_total  NUMERIC(18,2) NOT NULL DEFAULT 0,
    last_updated           TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT pk_fact_trip_minute PRIMARY KEY (bucket_start, service_type, service_tier, area_code)
);

CREATE INDEX IF NOT EXISTS ix_fact_trip_minute_bucket ON mart.fact_trip_minute(bucket_start DESC);
CREATE INDEX IF NOT EXISTS ix_fact_trip_minute_scope  ON mart.fact_trip_minute(service_type, service_tier, area_code);

-- =========================
-- KPI LIVE (cộng dồn realtime)
-- =========================
DROP TABLE IF EXISTS mart.latest_kpi_by_scope CASCADE;
CREATE TABLE mart.latest_kpi_by_scope (
    kpi_name     TEXT        NOT NULL,  -- total_requests, accepted_total, completed_total, canceled_total, revenue_total, discount_total, gmv_total
    service_type TEXT        NOT NULL DEFAULT '',
    service_tier TEXT        NOT NULL DEFAULT '',
    area_code    TEXT        NOT NULL DEFAULT '',
    kpi_value    NUMERIC(18,2) NOT NULL DEFAULT 0,
    event_time   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    last_updated TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT uq_latest_kpi_scope UNIQUE (kpi_name, service_type, service_tier, area_code)
);

CREATE INDEX IF NOT EXISTS ix_latest_kpi_name ON mart.latest_kpi_by_scope(kpi_name);

-- =========================
-- VIEW thuận tiện cho BI
-- =========================
DROP VIEW IF EXISTS public.realtime_kpi_metrics;
CREATE VIEW public.realtime_kpi_metrics AS
SELECT
  kpi_name,
  service_type,
  service_tier,
  area_code,
  kpi_value,
  event_time,
  last_updated
FROM mart.latest_kpi_by_scope;

-- (tuỳ chọn) quyền public chỉ đọc view
-- GRANT SELECT ON public.realtime_kpi_metrics TO PUBLIC;
