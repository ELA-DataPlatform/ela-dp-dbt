{{
    config(
        materialized='table',
        tags=['garmin', 'webapp']
    )
}}

WITH date_spine AS (
    SELECT
        date_add(
            date_sub(current_date('Europe/Paris'), INTERVAL 13 DAY),
            INTERVAL n DAY
        ) AS activity_date
    FROM unnest(generate_array(0, 13)) AS n
),

runs_daily AS (
    SELECT
        activity_date,
        sum(performance.distance_m) / 1000.0 AS distance_km
    FROM {{ ref('svc_hub__master_running_activities') }}
    WHERE activity_date >= date_sub(current_date('Europe/Paris'), INTERVAL 13 DAY)
    GROUP BY activity_date
),

joined AS (
    SELECT
        d.activity_date,
        round(coalesce(r.distance_km, 0), 2) AS distance_km,
        extract(ISOWEEK FROM d.activity_date) = extract(ISOWEEK FROM current_date('Europe/Paris'))
        AND extract(ISOYEAR FROM d.activity_date) = extract(ISOYEAR FROM current_date('Europe/Paris'))
            AS is_current_week
    FROM date_spine AS d
    LEFT JOIN runs_daily AS r ON d.activity_date = r.activity_date
),

week_totals AS (
    SELECT
        round(sum(CASE
            WHEN activity_date >= date_trunc(current_date('Europe/Paris'), ISOWEEK)
                THEN performance.distance_m / 1000.0
            ELSE 0
        END), 2) AS current_week_km,
        round(sum(CASE
            WHEN
                activity_date >= date_sub(date_trunc(current_date('Europe/Paris'), ISOWEEK), INTERVAL 7 DAY)
                AND activity_date < date_trunc(current_date('Europe/Paris'), ISOWEEK)
                THEN performance.distance_m / 1000.0
            ELSE 0
        END), 2) AS previous_week_km
    FROM {{ ref('svc_hub__master_running_activities') }}
    WHERE activity_date >= date_sub(date_trunc(current_date('Europe/Paris'), ISOWEEK), INTERVAL 7 DAY)
)

SELECT
    w.current_week_km,
    w.previous_week_km,
    round(safe_divide(w.current_week_km - w.previous_week_km, w.previous_week_km) * 100, 1) AS week_km_delta_pct,
    array(
        SELECT AS STRUCT
            j.activity_date,
            j.distance_km,
            j.is_current_week,
            concat(
                cast(extract(DAY FROM j.activity_date) AS string),
                '/',
                cast(extract(MONTH FROM j.activity_date) AS string)
            ) AS day_label,
            CASE extract(DAYOFWEEK FROM j.activity_date)
                WHEN 1 THEN 'D'
                WHEN 2 THEN 'L'
                WHEN 3 THEN 'M'
                WHEN 4 THEN 'M'
                WHEN 5 THEN 'J'
                WHEN 6 THEN 'V'
                WHEN 7 THEN 'S'
            END AS day_letter
        FROM joined AS j
        ORDER BY j.activity_date
    ) AS days
FROM week_totals AS w
