{{
    config(
        materialized='incremental',
        unique_key=['reference_activity_id', 'recent_activity_id'],
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
        hub.performance.duration_s,
        hub.performance.avg_pace_min_per_km,
        hub.performance.avg_hr_bpm,
        hub.performance.training_load,
        hub.is_pr,
        hub._ingested_at
    FROM {{ ref('svc_hub__master_running_activities') }} AS hub
),

ranked AS (
    SELECT
        a.activity_id AS reference_activity_id,
        b.activity_id AS recent_activity_id,
        b.activity_date,
        b.start_time_local,
        ROUND(b.distance_m / 1000.0, 2) AS distance_km,
        CAST(ROUND(b.duration_s) AS INT64) AS duration_seconds,
        {{ format_duration_label('b.duration_s') }}                 AS duration_label,
        b.avg_pace_min_per_km,
        CASE
            WHEN
                b.avg_pace_min_per_km IS NOT NULL
                AND b.avg_pace_min_per_km > 0
                THEN {{ format_pace_label('b.avg_pace_min_per_km') }}
        END AS pace_label,
        CAST(ROUND(b.avg_hr_bpm) AS INT64) AS avg_hr_bpm,
        b.training_load,
        b.is_pr,
        a._ingested_at,
        ROW_NUMBER() OVER (
            PARTITION BY a.activity_id
            ORDER BY b.start_time_local DESC
        ) AS recency_rank
    FROM base AS a
    INNER JOIN base AS b
        ON
            a.activity_type = b.activity_type
            AND a.activity_date > b.activity_date
)

SELECT
    ranked.reference_activity_id,
    ranked.recent_activity_id,
    ranked.activity_date,
    ranked.start_time_local,
    ranked.distance_km,
    ranked.duration_seconds,
    ranked.duration_label,
    ranked.avg_pace_min_per_km,
    ranked.pace_label,
    ranked.avg_hr_bpm,
    ranked.training_load,
    ranked.is_pr,
    ranked._ingested_at
FROM ranked
WHERE
    ranked.recency_rank <= 5

    {% if is_incremental() %}
        AND ranked._ingested_at > (SELECT MAX(_ingested_at) FROM {{ this }})
    {% endif %}
