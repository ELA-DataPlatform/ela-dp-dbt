{{
    config(
        alias='rhr_daily',
        materialized='table',
        tags=['garmin', 'data4agent']
    )
}}

SELECT
    date AS rhr_date,
    resting_heart_rate AS resting_hr_bpm,
    _ingested_at
FROM {{ ref('dlk_garmin_svc__rhr_daily') }}
WHERE date IS NOT NULL AND date >= '2025-01-01'
