{{
    config(
        materialized='table',
        tags=['garmin', 'webapp', 'spotify']
    )
}}

WITH activities AS (
    SELECT activity_id
    FROM {{ ref('hub_activities_svc__master_running_activities') }}
),

music AS (
    SELECT
        activity_id,
        COUNT(*) AS track_count,
        CAST(ROUND(SUM(duration_seconds) / 60) AS INT64) AS total_duration_minutes
    FROM {{ ref('pct_webapp_activity_focus__music_timeline') }}
    GROUP BY activity_id
)

SELECT
    a.activity_id,
    COALESCE(m.track_count, 0) AS track_count,
    COALESCE(m.total_duration_minutes, 0) AS total_duration_minutes
FROM activities AS a
LEFT JOIN music AS m ON a.activity_id = m.activity_id
