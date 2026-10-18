{{
    config(
        alias='stress_daily',
        materialized='table',
        tags=['garmin', 'data4agent']
    )
}}

SELECT
    health_date AS stress_date,
    calendar_date,
    avg_stress_level,
    max_stress_level,
    start_at_utc AS start_timestamp_gmt,
    end_at_utc AS end_timestamp_gmt,
    _ingested_at,
    TIMESTAMP(start_at_local) AS start_timestamp_local,
    TIMESTAMP(end_at_local) AS end_timestamp_local
FROM {{ ref('hub_health_svc__stress_daily') }}
WHERE health_date IS NOT NULL AND health_date >= '2025-01-01'
