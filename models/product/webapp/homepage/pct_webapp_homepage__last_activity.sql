{{
    config(
        materialized='table',
        tags=['garmin', 'webapp']
    )
}}

WITH last_activity AS (
    SELECT
        cast(activity_id AS string) AS activity_id,
        activity_name,
        activity_date,
        start_time_local,
        performance.distance_m,
        performance.duration_s,
        performance.avg_pace_min_per_km,
        performance.avg_hr_bpm,
        performance.elevation_gain_m,
        timeseries
    FROM {{ ref('svc_hub__master_running_activities') }}
    ORDER BY activity_date DESC, start_time_local DESC
    LIMIT 1
),

gps_points AS (
    SELECT
        ts.latitude,
        ts.longitude,
        ts_offset AS point_index,
        COUNT(*) OVER () AS total_points
    FROM last_activity
    CROSS JOIN UNNEST(timeseries) AS ts WITH OFFSET AS ts_offset
    WHERE ts.latitude IS NOT NULL AND ts.longitude IS NOT NULL
),

sampled_gps AS (
    SELECT latitude, longitude, point_index
    FROM gps_points
    WHERE MOD(point_index, GREATEST(1, CAST(CEIL(total_points / 400.0) AS INT64))) = 0
)

SELECT
    activity_id,
    activity_name,
    activity_date,
    cast(round(duration_s) AS int64) AS duration_seconds,
    cast(round(avg_pace_min_per_km * 60) AS int64) AS pace_seconds_per_km,
    cast(round(avg_hr_bpm) AS int64) AS avg_heart_rate_bpm,
    cast(round(elevation_gain_m) AS int64) AS elevation_gain_m,
    (
        SELECT
            concat(
                '[',
                string_agg(
                    concat('[', cast(latitude AS string), ',', cast(longitude AS string), ']'),
                    ',' ORDER BY point_index
                ),
                ']'
            )
        FROM sampled_gps
    ) AS polyline,
    concat(
        cast(extract(DAY FROM activity_date) AS string),
        ' ',
        CASE extract(MONTH FROM activity_date)
            WHEN 1 THEN 'janv.'
            WHEN 2 THEN 'févr.'
            WHEN 3 THEN 'mars'
            WHEN 4 THEN 'avr.'
            WHEN 5 THEN 'mai'
            WHEN 6 THEN 'juin'
            WHEN 7 THEN 'juil.'
            WHEN 8 THEN 'août'
            WHEN 9 THEN 'sept.'
            WHEN 10 THEN 'oct.'
            WHEN 11 THEN 'nov.'
            WHEN 12 THEN 'déc.'
        END,
        ' ',
        cast(extract(YEAR FROM activity_date) AS string)
    ) AS activity_date_label,
    date_diff(current_date('Europe/Paris'), activity_date, DAY) AS days_ago,
    round(distance_m / 1000.0, 2) AS distance_km,
    CASE
        WHEN duration_s >= 3600
            THEN concat(
                cast(cast(duration_s / 3600 AS int64) AS string),
                'h ',
                format('%02d', cast(mod(cast(round(duration_s) AS int64), 3600) / 60 AS int64))
            )
        ELSE concat(
            cast(cast(duration_s / 60 AS int64) AS string),
            ' min'
        )
    END AS duration_label,
    concat(
        cast(cast(avg_pace_min_per_km AS int64) AS string),
        "'",
        format('%02d', cast(
            round((avg_pace_min_per_km - cast(avg_pace_min_per_km AS int64)) * 60)
            AS int64
        )),
        '"/km'
    ) AS pace_label
FROM last_activity
