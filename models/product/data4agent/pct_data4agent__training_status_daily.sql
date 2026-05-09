{{
    config(
        alias='training_status_daily',
        materialized='table',
        tags=['garmin', 'data4agent']
    )
}}

SELECT  -- noqa: ST06
    date AS status_date,
    calendar_date,
    training_status AS status_code,
    CASE training_status
        WHEN 0 THEN 'NO_STATUS'
        WHEN 1 THEN 'DETRAINING'
        WHEN 2 THEN 'RECOVERY'
        WHEN 3 THEN 'MAINTAINING'
        WHEN 4 THEN 'PRODUCTIVE'
        WHEN 5 THEN 'PEAKING'
        WHEN 6 THEN 'OVERREACHING'
        WHEN 7 THEN 'UNPRODUCTIVE'
        WHEN 8 THEN 'STRAINED'
    END AS status_label,
    training_status_feedback_phrase AS feedback_phrase,
    training_paused,
    sport,
    sub_sport,
    fitness_trend_sport,
    fitness_trend,
    acwr_status,
    acwr_status_feedback,
    acwr_percent,
    daily_acute_chronic_workload_ratio AS acwr_ratio,
    daily_training_load_acute AS acute_load,
    daily_training_load_chronic AS chronic_load,
    min_training_load_chronic AS chronic_load_lower_bound,
    max_training_load_chronic AS chronic_load_upper_bound,
    since_date,
    device_id,
    _ingested_at
FROM {{ ref('svc_garmin__training_status') }}
WHERE date IS NOT NULL AND date >= '2025-01-01'
