-- Dataset 1: Realtime KPIs
CREATE OR REPLACE VIEW mart.dashboard_operations_realtime AS
SELECT 
    metric_name,
    service_type,
    service_tier,
    area_code,
    metric_value,
    numerator_value,
    denominator_value,
    metric_unit,
    event_time,
    last_updated
FROM mart.latest_metric_snapshot
WHERE metric_name IN (
    'request_order',
    'accept_order', 
    'acceptance_rate',
    'completed_order',
    'completed_rate',
    'gsv',
    'revenue_vat',
    'revenue_net',
    'cancel_order',
    'cancel_rate',
    'order_time',
    'board_time',
    'lead_time',
    'accept_time'
)
AND last_updated >= NOW() - INTERVAL '5 minutes';

-- Dataset 2: Hourly Trends (last 24 hours)
CREATE OR REPLACE VIEW mart.dashboard_operations_hourly AS
SELECT 
    bucket_hour,
    metric_name,
    service_type,
    service_tier,
    area_code,
    metric_value,
    numerator_value,
    denominator_value
FROM mart.vw_metric_hourly
WHERE bucket_hour >= NOW() - INTERVAL '24 hours'
AND metric_name IN (
    'request_order',
    'accept_order',
    'completed_order',
    'cancel_order'
);

-- Driver analytics dataset
CREATE OR REPLACE VIEW mart.dashboard_driver_analytics AS
WITH realtime_metrics AS (
    SELECT 
        metric_name,
        service_type,
        service_tier,
        area_code,
        metric_value,
        last_updated
    FROM mart.latest_metric_snapshot
    WHERE metric_name IN ('online_driver', 'busy_driver', 'net_driver_income')
),
daily_metrics AS (
    SELECT 
        bucket_day::date AS date,
        metric_name,
        service_type,
        service_tier,
        area_code,
        SUM(metric_value) AS metric_value
    FROM mart.vw_metric_daily
    WHERE metric_name IN (
        'new_driver',
        'active_driver',
        'retention_driver',
        'churn_driver',
        'productivity'
    )
    AND bucket_day >= CURRENT_DATE - INTERVAL '90 days'
    GROUP BY 1,2,3,4,5
)
SELECT * FROM realtime_metrics
UNION ALL
SELECT 
    metric_name,
    service_type,
    service_tier,
    area_code,
    metric_value,
    date::timestamp AS last_updated
FROM daily_metrics;

-- Passenger analytics dataset
CREATE OR REPLACE VIEW mart.dashboard_passenger_analytics AS
SELECT 
    bucket_day::date AS date,
    metric_name,
    cohort_key,
    SUM(metric_value) AS metric_value,
    SUM(numerator_value) AS numerator_value,
    SUM(denominator_value) AS denominator_value
FROM mart.vw_metric_daily
WHERE metric_name IN (
    'new_passenger',
    'active_passenger',
    'retention_passenger',
    'churn_passenger',
    'return_passenger'
)
AND bucket_day >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY 1, 2, 3;
