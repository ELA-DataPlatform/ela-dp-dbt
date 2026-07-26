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
        {{ meters_to_kilometers('sum(performance.distance_m)') }} AS distance_km
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
                THEN {{ meters_to_kilometers('performance.distance_m') }}
            ELSE 0
        END), 2) AS current_week_km,
        round(sum(CASE
            WHEN
                activity_date >= date_sub(date_trunc(current_date('Europe/Paris'), ISOWEEK), INTERVAL 7 DAY)
                AND activity_date < date_trunc(current_date('Europe/Paris'), ISOWEEK)
                THEN {{ meters_to_kilometers('performance.distance_m') }}
            ELSE 0
        END), 2) AS previous_week_km
    FROM {{ ref('svc_hub__master_running_activities') }}
    WHERE activity_date >= date_sub(date_trunc(current_date('Europe/Paris'), ISOWEEK), INTERVAL 7 DAY)
)

SELECT
    w.current_week_km,
    w.previous_week_km,
    round(safe_divide(w.current_week_km - w.previous_week_km, w.previous_week_km) * 100, 1) AS week_km_delta_pct,
    CASE
        WHEN w.previous_week_km > 0
            THEN concat(
                if(w.current_week_km >= w.previous_week_km, '+', ''),
                cast(
                    cast(
                        round(
                            safe_divide(
                                w.current_week_km - w.previous_week_km,
                                w.previous_week_km
                            ) * 100
                        ) AS INT64
                    ) AS STRING
                ),
                '% vs sem. préc.'
            )
        ELSE '— vs sem. préc.'
    END AS week_km_delta_label,
    CASE
        WHEN w.current_week_km >= w.previous_week_km THEN 'success'
        ELSE 'danger'
    END AS week_km_delta_tone,
    array(
        SELECT AS STRUCT
            j.activity_date,
            j.distance_km,
            j.is_current_week,
            {{ format_day_label('j.activity_date') }} AS day_label,
            {{ dayofweek_to_french_letter('j.activity_date') }} AS day_letter
        FROM joined AS j
        ORDER BY j.activity_date
    ) AS days
FROM week_totals AS w
