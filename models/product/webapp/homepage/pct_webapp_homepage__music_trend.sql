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

streams_raw AS (
    SELECT
        date(fp.played_at, 'Europe/Paris') AS stream_date,
        coalesce(t.duration_ms, 0) AS duration_ms
    FROM {{ ref('svc_hub__fact_played') }} AS fp
    LEFT JOIN {{ ref('svc_hub__ref_track') }} AS t ON fp.track_id = t.track_id
    WHERE date(fp.played_at, 'Europe/Paris') >= date_sub(current_date('Europe/Paris'), INTERVAL 9 DAY)
),

streams_current AS (
    SELECT
        stream_date,
        {{ milliseconds_to_minutes('sum(duration_ms)') }} AS listening_minutes
    FROM streams_raw
    GROUP BY stream_date
),

streams_prev AS (
    SELECT {{ milliseconds_to_minutes('sum(t.duration_ms)') }} AS total_minutes_prev_10d
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
    SELECT
        cast(round(avg(listening_minutes)) AS INT64) AS avg_daily_listening_minutes_10d,
        sum(listening_minutes) AS total_minutes_10d
    FROM joined
)

SELECT
    -- ── KPIs haut niveau (plats) ─────────────────────────────────────────────────
    t.total_minutes_10d,
    t.avg_daily_listening_minutes_10d,
    p.total_minutes_prev_10d,
    {{ minutes_to_label('t.total_minutes_10d') }} AS total_label,
    CASE
        WHEN p.total_minutes_prev_10d > 0
            THEN round(
                (cast(t.total_minutes_10d AS FLOAT64) - cast(p.total_minutes_prev_10d AS FLOAT64))
                / cast(p.total_minutes_prev_10d AS FLOAT64) * 100,
                1
            )
    END AS delta_pct,
    CASE
        WHEN p.total_minutes_prev_10d > 0
            THEN concat(
                if(t.total_minutes_10d >= p.total_minutes_prev_10d, '+', ''),
                cast(
                    cast(
                        round(
                            (
                                cast(t.total_minutes_10d AS FLOAT64)
                                - cast(p.total_minutes_prev_10d AS FLOAT64)
                            )
                            / cast(p.total_minutes_prev_10d AS FLOAT64) * 100
                        ) AS INT64
                    ) AS STRING
                ),
                '% vs préc.'
            )
        ELSE '— vs préc.'
    END AS delta_label,
    CASE
        WHEN p.total_minutes_prev_10d IS NULL OR p.total_minutes_prev_10d = 0 THEN 'neutral'
        WHEN cast(t.total_minutes_10d AS FLOAT64) / cast(p.total_minutes_prev_10d AS FLOAT64) >= 1.05 THEN 'success'
        WHEN cast(t.total_minutes_10d AS FLOAT64) / cast(p.total_minutes_prev_10d AS FLOAT64) <= 0.95 THEN 'danger'
        ELSE 'neutral'
    END AS delta_tone,

    -- ── Détail quotidien (ARRAY<STRUCT>) ─────────────────────────────────────────
    array(
        SELECT AS STRUCT
            j.stream_date,
            j.listening_minutes,
            {{ format_day_label('j.stream_date') }} AS day_label,
            {{ dayofweek_to_french_letter('j.stream_date') }} AS day_letter
        FROM joined AS j
        ORDER BY j.stream_date
    ) AS days
FROM totals AS t
CROSS JOIN streams_prev AS p
