{{
    config(
        alias='hrv_daily',
        materialized='table',
        tags=['garmin', 'data4agent']
    )
}}

SELECT
    health_date AS hrv_date,
    hrv_calendar_date,
    hrv_status,
    hrv_feedback_phrase,
    hrv_last_night_avg,
    hrv_last_night_5min_high,
    hrv_weekly_avg,
    hrv_baseline_low_upper,
    hrv_baseline_balanced_low,
    hrv_baseline_balanced_upper,
    hrv_baseline_marker_value,
    _ingested_at
FROM {{ ref('hub_health_svc__hrv_daily') }}
WHERE health_date IS NOT NULL AND health_date >= '2025-01-01'
