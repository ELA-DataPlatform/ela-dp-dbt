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
)

SELECT
    activity_id,
    activity_name,
    activity_date,
    cast(round(duration_s) AS int64) AS duration_seconds,
    cast({{ minutes_per_km_to_seconds_per_km('avg_pace_min_per_km') }} AS int64) AS pace_seconds_per_km,
    cast(round(avg_hr_bpm) AS int64) AS avg_heart_rate_bpm,
    cast(round(elevation_gain_m) AS int64) AS elevation_gain_m,
    (
        SELECT
            concat(
                '[',
                string_agg(
                    concat('[', cast(ts.latitude AS string), ',', cast(ts.longitude AS string), ']'),
                    ','
                ),
                ']'
            )
        FROM unnest(timeseries) AS ts
        WHERE ts.latitude IS NOT NULL AND ts.longitude IS NOT NULL
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
    {{ meters_to_kilometers('distance_m', 2) }} AS distance_km,
    {{ format_duration_label('duration_s') }} AS duration_label,
    {{ format_pace_label('avg_pace_min_per_km') }} AS pace_label
FROM last_activity
