{{
    config(
        materialized='table',
        tags=['garmin', 'webapp']
    )
}}

WITH latest_ts AS (
    SELECT *
    FROM {{ ref('hub_health_svc__training_status_daily') }}
    QUALIFY row_number() OVER (
        ORDER BY health_date DESC
    ) = 1
),

latest_mm AS (
    SELECT
        health_date,
        vo2_max,
        vo2_max_precise
    FROM {{ ref('hub_health_svc__vo2max_daily') }}
    QUALIFY row_number() OVER (
        ORDER BY health_date DESC
    ) = 1
),

prev_mm AS (
    SELECT
        vo2_max,
        vo2_max_precise
    FROM {{ ref('hub_health_svc__vo2max_daily') }}
    QUALIFY row_number() OVER (
        ORDER BY health_date DESC
    ) = 2
)

SELECT
    ts.health_date AS snapshot_date,
    ts.acute_load AS training_load_7d,
    cast(ts.chronic_load_lower_bound AS int64) AS training_load_target_low,
    cast(ts.chronic_load_upper_bound AS int64) AS training_load_target_high,
    cast(NULL AS string) AS active_goal_name,
    cast(NULL AS date) AS active_goal_date,
    cast(NULL AS int64) AS active_goal_days_left,
    CASE
        WHEN
            ts.feedback_phrase LIKE 'PRODUCTIVE%'
            OR ts.feedback_phrase LIKE 'PEAKING%' THEN 'productive'
        WHEN
            ts.feedback_phrase LIKE 'RECOVERY%'
            OR ts.feedback_phrase LIKE 'RECOVERING%' THEN 'recovery'
        WHEN ts.feedback_phrase LIKE 'DETRAINING%' THEN 'detraining'
        WHEN
            ts.feedback_phrase LIKE 'MAINTAINING%'
            OR ts.feedback_phrase LIKE 'UNPRODUCTIVE%' THEN 'maintaining'
        WHEN
            ts.feedback_phrase LIKE 'OVERREACHING%'
            OR ts.feedback_phrase LIKE 'OVERTRAINING%' THEN 'overreaching'
        ELSE 'maintaining'
    END AS training_state,
    round(coalesce(mm.vo2_max_precise, mm.vo2_max), 1) AS vo2_max,
    CASE
        WHEN mm.vo2_max_precise > pm.vo2_max_precise THEN 'improving'
        WHEN mm.vo2_max_precise < pm.vo2_max_precise THEN 'declining'
        ELSE 'stable'
    END AS vo2_max_status
FROM latest_ts AS ts
LEFT JOIN latest_mm AS mm ON TRUE
LEFT JOIN prev_mm AS pm ON TRUE
