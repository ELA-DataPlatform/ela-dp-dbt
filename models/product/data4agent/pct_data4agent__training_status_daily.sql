{{
    config(
        alias='training_status_daily',
        materialized='table',
        tags=['garmin', 'data4agent']
    )
}}

SELECT
    health_date AS status_date,
    status_code,
    status_label,
    feedback_phrase,
    training_paused,
    sport,
    sub_sport,
    fitness_trend_sport,
    fitness_trend,
    acwr_status,
    acwr_status_feedback,
    acwr_percent,
    acwr_ratio,
    acute_load,
    chronic_load,
    chronic_load_lower_bound,
    chronic_load_upper_bound,
    device_id,
    _ingested_at,
    CAST(calendar_date AS STRING) AS calendar_date,
    CAST(since_date AS STRING) AS since_date
FROM {{ ref('hub_health_svc__training_status_daily') }}
WHERE health_date IS NOT NULL AND health_date >= '2025-01-01'
