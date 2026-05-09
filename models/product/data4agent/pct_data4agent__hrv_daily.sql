{{
    config(
        alias='hrv_daily',
        materialized='table',
        tags=['garmin', 'data4agent']
    )
}}

SELECT
    date AS hrv_date,
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
FROM {{ ref('svc_garmin__hrv') }}
WHERE date IS NOT NULL AND date >= '2025-01-01'
