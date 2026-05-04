{{
    config(
        materialized='table',
        tags=['spotify', 'webapp']
    )
}}

WITH date_spine AS (
    SELECT
        date_add(
            date_sub(current_date('Europe/Paris'), INTERVAL 9 DAY),
            INTERVAL n DAY
        ) AS stream_date
    FROM unnest(generate_array(0, 9)) AS n
),

streams_current AS (
    SELECT
        date(fp.played_at, 'Europe/Paris') AS stream_date,
        cast(sum(t.duration_ms) / 60000 AS int64) AS listening_minutes
    FROM {{ ref('svc_hub__fact_played') }} AS fp
    LEFT JOIN {{ ref('svc_hub__ref_track') }} AS t ON fp.track_id = t.track_id
    WHERE date(fp.played_at, 'Europe/Paris') >= date_sub(current_date('Europe/Paris'), INTERVAL 9 DAY)
    GROUP BY 1
),

streams_prev AS (
    SELECT cast(sum(t.duration_ms) / 60000 AS int64) AS total_minutes_prev_10d
    FROM {{ ref('svc_hub__fact_played') }} AS fp
    LEFT JOIN {{ ref('svc_hub__ref_track') }} AS t ON fp.track_id = t.track_id
    WHERE
        date(fp.played_at, 'Europe/Paris') >= date_sub(current_date('Europe/Paris'), INTERVAL 19 DAY)
        AND date(fp.played_at, 'Europe/Paris') < date_sub(current_date('Europe/Paris'), INTERVAL 9 DAY)
),

joined AS (
    SELECT
        d.stream_date,
        coalesce(s.listening_minutes, 0) AS listening_minutes
    FROM date_spine AS d
    LEFT JOIN streams_current AS s ON d.stream_date = s.stream_date
),

totals AS (
    SELECT sum(listening_minutes) AS total_minutes_10d
    FROM joined
)

SELECT
    j.stream_date,
    j.listening_minutes,
    t.total_minutes_10d,
    p.total_minutes_prev_10d,
    concat(
        cast(extract(DAY FROM j.stream_date) AS string),
        '/',
        cast(extract(MONTH FROM j.stream_date) AS string)
    ) AS day_label,
    concat(
        cast(cast(t.total_minutes_10d / 60 AS int64) AS string),
        'h',
        format('%02d', mod(cast(t.total_minutes_10d AS int64), 60))
    ) AS total_label,
    CASE
        WHEN p.total_minutes_prev_10d > 0
            THEN round(
                (cast(t.total_minutes_10d AS float64) - cast(p.total_minutes_prev_10d AS float64))
                / cast(p.total_minutes_prev_10d AS float64) * 100,
                1
            )
    END AS delta_pct,
    CASE
        WHEN p.total_minutes_prev_10d IS NULL OR p.total_minutes_prev_10d = 0 THEN 'neutral'
        WHEN cast(t.total_minutes_10d AS float64) / cast(p.total_minutes_prev_10d AS float64) >= 1.05 THEN 'success'
        WHEN cast(t.total_minutes_10d AS float64) / cast(p.total_minutes_prev_10d AS float64) <= 0.95 THEN 'danger'
        ELSE 'neutral'
    END AS delta_tone
FROM joined AS j
CROSS JOIN totals AS t
CROSS JOIN streams_prev AS p
ORDER BY j.stream_date
