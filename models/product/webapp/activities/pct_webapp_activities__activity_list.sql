{{
    config(
        materialized='table',
        tags=['garmin', 'webapp']
    )
}}

SELECT  -- noqa: ST06
    CAST(activityid AS STRING) AS activity_id,
    activityname AS activity_name,
    activity_type.type_key AS activity_type,
    DATE(starttimelocal) AS activity_date,
    starttimelocal AS start_time_local,

    {{ meters_to_kilometers('distance', 2) }} AS distance_km,

    CAST(ROUND(duration) AS INT64) AS duration_seconds,
    {{ format_duration_label('duration') }} AS duration_label,

    {{ speed_mps_to_kmh('averagespeed') }} AS avg_speed_km_h,
    CAST(
        ROUND({{ speed_mps_to_pace_min_per_km('averagespeed') }} * 60)
        AS INT64
    ) AS pace_seconds_per_km,

    CASE
        WHEN activity_type.type_key IN ('tennis_v2', 'badminton', 'table_tennis', 'volleyball')
            THEN NULL
        WHEN activity_type.type_key IN ('cycling', 'indoor_cycling')
            THEN CONCAT(CAST({{ speed_mps_to_kmh('averagespeed', 1) }} AS STRING), ' km/h')
        WHEN averagespeed IS NOT NULL AND averagespeed > 0
            THEN {{ format_pace_label(speed_mps_to_pace_min_per_km('averagespeed')) }}
    END AS pace_label,

    CAST(ROUND(elevationgain) AS INT64) AS elevation_gain_m,
    CAST(ROUND(averagehr) AS INT64) AS avg_hr_bpm

FROM {{ ref('dlk_garmin_svc__activities') }}
ORDER BY starttimelocal DESC
