{{
    config(
        materialized='incremental',
        unique_key='activity_id',
        incremental_strategy='merge',
        tags=['garmin', 'webapp']
    )
}}

WITH base AS (
    SELECT
        hub.activity_id,
        hub.activity_type,
        hub.activity_date,
        hub.start_time_local,
        hub.performance.distance_m,
        hub.performance.avg_pace_min_per_km,
        hub.performance.avg_hr_bpm,
        hub.performance.training_load,
        hub._ingested_at
    FROM {{ ref('hub_activities_svc__master_running_activities') }} AS hub
),

with_previous AS (
    SELECT
        base.activity_id,
        base.activity_date,
        base.distance_m,
        base.avg_pace_min_per_km,
        base.avg_hr_bpm,
        base.training_load,
        base._ingested_at,
        LAG(base.distance_m) OVER (
            PARTITION BY base.activity_type
            ORDER BY base.start_time_local
        ) AS prev_distance_m,
        LAG(base.avg_pace_min_per_km) OVER (
            PARTITION BY base.activity_type
            ORDER BY base.start_time_local
        ) AS prev_avg_pace_min_per_km,
        LAG(base.avg_hr_bpm) OVER (
            PARTITION BY base.activity_type
            ORDER BY base.start_time_local
        ) AS prev_avg_hr_bpm,
        LAG(base.training_load) OVER (
            PARTITION BY base.activity_type
            ORDER BY base.start_time_local
        ) AS prev_training_load
    FROM base
)

SELECT
    wp.activity_id,
    wp.activity_date,
    wp._ingested_at,
    ROUND(SAFE_DIVIDE(wp.distance_m - wp.prev_distance_m, wp.prev_distance_m) * 100, 1) AS delta_distance_pct,
    {{ minutes_per_km_to_seconds_per_km(
        'wp.avg_pace_min_per_km - wp.prev_avg_pace_min_per_km'
    ) }} AS delta_pace_seconds,
    ROUND(wp.avg_hr_bpm - wp.prev_avg_hr_bpm, 0) AS delta_hr_bpm,
    ROUND(SAFE_DIVIDE(wp.training_load - wp.prev_training_load, wp.prev_training_load) * 100, 1)
        AS delta_training_load_pct
FROM with_previous AS wp

{% if is_incremental() %}
    WHERE wp._ingested_at > (SELECT MAX(_ingested_at) FROM {{ this }})
{% endif %}
