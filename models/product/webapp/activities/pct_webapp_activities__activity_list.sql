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

    ROUND(distance / 1000.0, 2) AS distance_km,

    CAST(ROUND(duration) AS INT64) AS duration_seconds,
    CASE
        WHEN duration >= 3600
            THEN CONCAT(
                CAST(DIV(CAST(ROUND(duration) AS INT64), 3600) AS STRING),
                'h ',
                FORMAT('%02d', DIV(MOD(CAST(ROUND(duration) AS INT64), 3600), 60))
            )
        ELSE CONCAT(
            CAST(DIV(CAST(ROUND(duration) AS INT64), 60) AS STRING),
            ' min'
        )
    END AS duration_label,

    ROUND(averagespeed * 3.6, 2) AS avg_speed_km_h,

    CASE
        WHEN activity_type.type_key IN ('tennis_v2', 'badminton', 'table_tennis', 'volleyball')
            THEN NULL
        WHEN activity_type.type_key IN ('cycling', 'indoor_cycling')
            THEN CONCAT(CAST(ROUND(averagespeed * 3.6, 1) AS STRING), ' km/h')
        WHEN averagespeed IS NOT NULL AND averagespeed > 0
            THEN CONCAT(
                CAST(CAST(SAFE_DIVIDE(1000.0, averagespeed) / 60.0 AS INT64) AS STRING),
                "'",
                FORMAT('%02d', CAST(MOD(CAST(ROUND(SAFE_DIVIDE(1000.0, averagespeed)) AS INT64), 60) AS INT64)),
                '"/km'
            )
    END AS pace_label,

    CAST(ROUND(elevationgain) AS INT64) AS elevation_gain_m,
    CAST(ROUND(averagehr) AS INT64) AS avg_hr_bpm

FROM {{ ref('svc_garmin__activities') }}
ORDER BY starttimelocal DESC
