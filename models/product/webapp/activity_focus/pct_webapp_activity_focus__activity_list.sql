{{
    config(
        materialized='incremental',
        unique_key='activity_id',
        incremental_strategy='merge',
        tags=['garmin', 'webapp']
    )
}}

SELECT
    hub.activity_id,
    hub.activity_name,
    hub.activity_type,
    hub.activity_date,
    hub.start_time_local,

    ROUND(hub.performance.distance_m / 1000.0, 2) AS distance_km,
    CAST(ROUND(hub.performance.duration_s) AS INT64) AS duration_seconds,
    {{ format_duration_label('hub.performance.duration_s') }}        AS duration_label,

    hub.performance.avg_pace_min_per_km,
    CASE
        WHEN
            hub.performance.avg_pace_min_per_km IS NOT NULL
            AND hub.performance.avg_pace_min_per_km > 0
            THEN {{ format_pace_label('hub.performance.avg_pace_min_per_km') }}
    END AS pace_label,

    CAST(ROUND(hub.performance.avg_hr_bpm) AS INT64) AS avg_hr_bpm,
    CAST(ROUND(hub.performance.elevation_gain_m) AS INT64) AS elevation_gain_m,
    hub.is_pr,
    hub._ingested_at

FROM {{ ref('svc_hub__master_running_activities') }} AS hub

{% if is_incremental() %}
    WHERE hub._ingested_at > (SELECT MAX(_ingested_at) FROM {{ this }})
{% endif %}
