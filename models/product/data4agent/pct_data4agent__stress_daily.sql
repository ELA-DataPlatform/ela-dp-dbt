{{
    config(
        alias='stress_daily',
        materialized='table',
        tags=['garmin', 'data4agent']
    )
}}

SELECT
    date AS stress_date,
    calendardate AS calendar_date,
    avgstresslevel AS avg_stress_level,
    maxstresslevel AS max_stress_level,
    starttimestamplocal AS start_timestamp_local,
    endtimestamplocal AS end_timestamp_local,
    starttimestampgmt AS start_timestamp_gmt,
    endtimestampgmt AS end_timestamp_gmt,
    _ingested_at
FROM {{ ref('dlk_garmin_svc__stress') }}
WHERE date IS NOT NULL AND date >= '2025-01-01'
