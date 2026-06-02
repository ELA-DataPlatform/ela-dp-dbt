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

WITH segments_raw AS (
    SELECT
        hub.activity_id,
        hub.activity_date,
        hub._ingested_at,
        lap.lap_index AS segment_index,
        lap.intensity_type,
        lap.distance,
        lap.duration,
        lap.average_speed,
        lap.avg_grade_adjusted_speed,
        lap.average_hr,
        lap.max_hr,
        lap.average_run_cadence,
        lap.elevation_gain,
        lap.elevation_loss,
        lap.calories,
        TIMESTAMP(lap.start_time_gmt) AS start_time_gmt,
        SUM(lap.distance) OVER (
            PARTITION BY hub.activity_id
            ORDER BY lap.lap_index
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cum_distance_m,
        COUNTIF(lap.intensity_type IN ('INTERVAL', 'ACTIVE')) OVER (
            PARTITION BY hub.activity_id
            ORDER BY lap.lap_index
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS active_rank
    FROM {{ ref('svc_hub__master_running_activities') }} AS hub
    CROSS JOIN UNNEST(hub.laps) AS lap
    WHERE
        lap.intensity_type IS NOT NULL

        {% if is_incremental() %}
            AND hub._ingested_at > (SELECT MAX(_ingested_at) FROM {{ this }})
        {% endif %}
)

SELECT
    sr.activity_id,
    sr.activity_date,

    sr.segment_index,
    sr.intensity_type,
    CASE sr.intensity_type
        WHEN 'WARMUP' THEN 'warmup'
        WHEN 'COOLDOWN' THEN 'cooldown'
        WHEN 'INTERVAL' THEN 'interval'
        WHEN 'ACTIVE' THEN 'interval'
        WHEN 'RECOVERY' THEN 'recovery'
        WHEN 'REST' THEN 'recovery'
    END AS segment_type,
    CASE sr.intensity_type
        WHEN 'WARMUP' THEN 'Échauffement'
        WHEN 'COOLDOWN' THEN 'Retour au calme'
        WHEN 'RECOVERY' THEN 'Récupération'
        WHEN 'REST' THEN 'Repos'
        WHEN 'INTERVAL' THEN CONCAT('Intervalle ', CAST(sr.active_rank AS STRING))
        WHEN 'ACTIVE' THEN CONCAT('Répétition ', CAST(sr.active_rank AS STRING))
    END AS segment_name,

    sr.start_time_gmt,
    ROUND((sr.cum_distance_m - sr.distance) / 1000.0, 3) AS start_km,
    ROUND(sr.cum_distance_m / 1000.0, 3) AS end_km,

    ROUND(sr.distance, 0) AS distance_m,
    CAST(ROUND(sr.duration) AS INT64) AS duration_seconds,
    {{ format_duration_label('sr.duration') }}                             AS duration_label,
    SAFE_DIVIDE(1000.0, sr.average_speed) / 60.0 AS pace_min_per_km,
    CASE
        WHEN SAFE_DIVIDE(1000.0, sr.average_speed) / 60.0 > 0
            THEN {{ format_pace_label('SAFE_DIVIDE(1000.0, sr.average_speed) / 60.0') }}
    END AS pace_label,
    SAFE_DIVIDE(1000.0, sr.avg_grade_adjusted_speed) / 60.0 AS gap_min_per_km,
    CAST(ROUND(sr.average_hr) AS INT64) AS avg_hr_bpm,
    CAST(ROUND(sr.max_hr) AS INT64) AS max_hr_bpm,
    ROUND(sr.average_run_cadence, 1) AS avg_cadence_spm,
    CAST(ROUND(sr.elevation_gain) AS INT64) AS elevation_gain_m,
    CAST(ROUND(sr.elevation_loss) AS INT64) AS elevation_loss_m,
    CAST(ROUND(sr.calories) AS INT64) AS calories,

    sr._ingested_at

FROM segments_raw AS sr
