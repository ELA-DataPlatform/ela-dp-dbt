{{
    config(
        materialized='table',
        tags=['garmin', 'webapp']
    )
}}

SELECT  -- noqa: ST06
    CAST(activity_id AS STRING) AS activity_id,
    identity.activity_name,
    sport.type_key AS activity_type,
    DATE(timing.start_at_local) AS activity_date,
    TIMESTAMP(timing.start_at_local) AS start_time_local,

    {{ meters_to_kilometers('performance.distance_m', 2) }} AS distance_km,

    CAST(ROUND(performance.duration_s) AS INT64) AS duration_seconds,
    {{ format_duration_label('performance.duration_s') }} AS duration_label,

    {{ speed_mps_to_kmh('performance.avg_speed_m_per_s') }} AS avg_speed_km_h,
    CAST(
        ROUND({{ speed_mps_to_pace_min_per_km('performance.avg_speed_m_per_s') }} * 60)
        AS INT64
    ) AS pace_seconds_per_km,

    CASE
        WHEN sport.type_key IN ('tennis_v2', 'badminton', 'table_tennis', 'volleyball')
            THEN NULL
        WHEN sport.type_key IN ('cycling', 'indoor_cycling')
            THEN CONCAT(CAST({{ speed_mps_to_kmh('performance.avg_speed_m_per_s', 1) }} AS STRING), ' km/h')
        WHEN performance.avg_speed_m_per_s IS NOT NULL AND performance.avg_speed_m_per_s > 0
            THEN {{ format_pace_label(speed_mps_to_pace_min_per_km('performance.avg_speed_m_per_s')) }}
    END AS pace_label,

    CAST(ROUND(performance.elevation_gain_m) AS INT64) AS elevation_gain_m,
    CAST(ROUND(performance.avg_hr_bpm) AS INT64) AS avg_hr_bpm

FROM {{ ref('hub_activities_svc__master_activities') }}
ORDER BY timing.start_at_local DESC
