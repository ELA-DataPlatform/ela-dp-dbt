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

    lap.lap_index,
    lap.intensity_type,
    ROUND(lap.distance, 0) AS distance_m,
    CAST(ROUND(lap.duration) AS INT64) AS duration_seconds,
    {{ format_duration_label('lap.duration') }} AS duration_label,
    {{ speed_mps_to_pace_min_per_km('lap.average_speed') }} AS pace_min_per_km,
    CAST(
        ROUND({{ speed_mps_to_pace_min_per_km('lap.average_speed') }} * 60)
        AS INT64
    ) AS pace_seconds_per_km,
    CASE
        WHEN {{ speed_mps_to_pace_min_per_km('lap.average_speed') }} > 0
            THEN {{ format_pace_label(speed_mps_to_pace_min_per_km('lap.average_speed')) }}
    END AS pace_label,
    {{ speed_mps_to_pace_min_per_km('lap.avg_grade_adjusted_speed') }} AS gap_min_per_km,
    CAST(
        ROUND({{ speed_mps_to_pace_min_per_km('lap.avg_grade_adjusted_speed') }} * 60)
        AS INT64
    ) AS gap_seconds_per_km,
    CAST(
        ROUND(
            COALESCE(
                {{ speed_mps_to_pace_min_per_km('lap.avg_grade_adjusted_speed') }},
                {{ speed_mps_to_pace_min_per_km('lap.average_speed') }}
            ) * 60
        )
        AS INT64
    ) AS display_gap_seconds_per_km,
    CAST(ROUND(lap.average_hr) AS INT64) AS avg_hr_bpm,
    CAST(ROUND(lap.max_hr) AS INT64) AS max_hr_bpm,
    ROUND(
        SAFE_DIVIDE(lap.average_hr, hub.performance.max_hr_bpm) * 100,
        1
    ) AS avg_hr_pct_of_activity_max,
    ROUND(lap.average_run_cadence, 1) AS avg_cadence_spm,
    CAST(ROUND(lap.elevation_gain) AS INT64) AS elevation_gain_m,
    CAST(ROUND(lap.elevation_loss) AS INT64) AS elevation_loss_m,
    CAST(ROUND(lap.calories) AS INT64) AS calories,
    ROUND(lap.average_power, 0) AS avg_power_w,
    ROUND(lap.normalized_power, 0) AS normalized_power_w,

    hub._ingested_at

FROM {{ ref('hub_activities_svc__master_running_activities') }} AS hub
CROSS JOIN UNNEST(hub.laps) AS lap

{% if is_incremental() %}
    WHERE
        hub._ingested_at > (SELECT MAX(_ingested_at) FROM {{ this }})
        AND lap.average_speed > 0
{% else %}
    WHERE lap.average_speed > 0
{% endif %}
