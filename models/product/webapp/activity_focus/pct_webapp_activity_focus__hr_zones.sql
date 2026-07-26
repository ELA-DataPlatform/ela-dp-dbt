{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={
            'field': 'activity_date',
            'data_type': 'date',
            'granularity': 'month'
        },
        cluster_by=['activity_id'],
        tags=['garmin', 'webapp']
    )
}}

SELECT
    hub.activity_id,
    hub.activity_date,

    hz.zone_number,
    CASE hz.zone_number
        WHEN 1 THEN 'Z1 — Récupération'
        WHEN 2 THEN 'Z2 — Endurance'
        WHEN 3 THEN 'Z3 — Tempo'
        WHEN 4 THEN 'Z4 — Seuil'
        WHEN 5 THEN 'Z5 — VO2max'
    END AS zone_name,
    hz.zone_low_boundary AS zone_min_bpm,
    LEAD(hz.zone_low_boundary) OVER (
        PARTITION BY hub.activity_id
        ORDER BY hz.zone_number
    ) AS zone_max_bpm,
    CAST(ROUND(COALESCE(
        LEAD(hz.zone_low_boundary) OVER (
            PARTITION BY hub.activity_id
            ORDER BY hz.zone_number
        ),
        GREATEST(hub.performance.max_hr_bpm, hz.zone_low_boundary)
    )) AS INT64) AS zone_display_max_bpm,
    hub.performance.max_hr_bpm AS athlete_max_hr_bpm,
    CAST(ROUND(hz.secs_in_zone) AS INT64) AS secs_in_zone,
    {{ format_duration_label('hz.secs_in_zone') }} AS time_in_zone_label,
    ROUND(SAFE_DIVIDE(hz.secs_in_zone, hub.performance.duration_s) * 100, 1) AS pct_time_in_zone,

    hub._ingested_at

FROM {{ ref('svc_hub__master_running_activities') }} AS hub
CROSS JOIN UNNEST(hub.hr_zones) AS hz

WHERE
    hz.zone_number IS NOT NULL

    {% if is_incremental() %}
        AND hub._ingested_at > (SELECT MAX(_ingested_at) FROM {{ this }})
    {% endif %}
