{{
    config(
        alias='rhr_daily',
        materialized='table',
        tags=['garmin', 'data4agent']
    )
}}

SELECT
    health_date AS rhr_date,
    resting_hr_bpm,
    _ingested_at
FROM {{ ref('hub_health_svc__rhr_daily') }}
WHERE health_date IS NOT NULL AND health_date >= '2025-01-01'
