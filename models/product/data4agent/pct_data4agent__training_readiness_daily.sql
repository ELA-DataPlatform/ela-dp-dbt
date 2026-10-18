{{
    config(
        alias='training_readiness_daily',
        materialized='table',
        tags=['garmin', 'data4agent']
    )
}}

SELECT
    health_date AS readiness_date,
    readiness_score,
    readiness_level,
    feedback_short,
    sleep_factor_pct,
    hrv_factor_pct,
    hrv_weekly_average_ms AS hrv_weekly_average,
    stress_history_factor_pct,
    recovery_time_factor_pct,
    recovery_time_hours,
    acwr_factor_pct,
    acute_load,
    _ingested_at
FROM {{ ref('hub_health_svc__training_readiness_daily') }}
WHERE health_date IS NOT NULL AND health_date >= '2025-01-01'
