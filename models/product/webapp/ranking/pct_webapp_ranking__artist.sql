{{
    config(
        materialized='table',
        tags=['spotify']
    )
}}

WITH fact_with_artist AS (
    SELECT
        fp.played_at,
        fp.track_id,
        t.duration_ms,
        bta.artist_id,
        DATE(fp.played_at, 'Europe/Paris') AS play_date
    FROM {{ ref('svc_hub__fact_played') }} AS fp
    LEFT JOIN {{ ref('svc_hub__ref_track') }} AS t ON fp.track_id = t.track_id
    INNER JOIN {{ ref('svc_hub__bridge_track_artist') }} AS bta
        ON fp.track_id = bta.track_id AND bta.artist_position = 0
),

current_agg AS (
    SELECT
        fa.artist_id,
        cp.period,
        COUNT(*) AS play_count,
        CAST(SUM(fa.duration_ms) / 60000 AS INT64) AS listening_time_min,
        COUNT(DISTINCT fa.track_id) AS unique_tracks,
        MAX(fa.played_at) AS last_played_at
    FROM fact_with_artist AS fa
    CROSS JOIN {{ ref('stg_hub__ref_calendar') }} AS cp
    WHERE
        (cp.period_start IS NULL OR fa.play_date >= cp.period_start)
        AND fa.play_date <= cp.period_end
    GROUP BY fa.artist_id, cp.period
),

prev_agg AS (
    SELECT
        fa.artist_id,
        cp.period,
        CAST(SUM(fa.duration_ms) / 60000 AS INT64) AS listening_time_min
    FROM fact_with_artist AS fa
    CROSS JOIN {{ ref('stg_hub__ref_calendar') }} AS cp
    WHERE
        cp.prev_period_start IS NOT NULL
        AND fa.play_date >= cp.prev_period_start
        AND fa.play_date <= cp.prev_period_end
    GROUP BY fa.artist_id, cp.period
),

current_ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY period ORDER BY listening_time_min DESC) AS rank
    FROM current_agg
),

prev_ranked AS (
    SELECT
        artist_id,
        period,
        ROW_NUMBER() OVER (PARTITION BY period ORDER BY listening_time_min DESC) AS rank_previous
    FROM prev_agg
),

artist_images AS (
    SELECT
        artist_id,
        JSON_VALUE(JSON_QUERY(images, '$[0]'), '$.url') AS artist_image_url
    FROM {{ ref('svc_spotify__artist_detail') }}
    WHERE images IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY _ingested_at DESC) = 1
)

SELECT
    c.period,
    c.rank,
    pr.rank_previous,
    c.artist_id,
    a.artist_name,
    ai.artist_image_url,
    c.play_count,
    c.listening_time_min,
    c.unique_tracks,
    c.last_played_at,
    (pr.rank_previous IS NULL AND c.period != 'all') AS is_new_entry
FROM current_ranked AS c
LEFT JOIN prev_ranked AS pr
    ON c.artist_id = pr.artist_id AND c.period = pr.period
LEFT JOIN {{ ref('svc_hub__ref_artist') }} AS a ON c.artist_id = a.artist_id
LEFT JOIN artist_images AS ai ON c.artist_id = ai.artist_id
WHERE c.rank <= 20
ORDER BY c.period, c.rank
