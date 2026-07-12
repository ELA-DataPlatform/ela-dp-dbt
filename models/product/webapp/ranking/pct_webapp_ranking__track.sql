{{
    config(
        materialized='table',
        tags=['spotify']
    )
}}

WITH current_agg AS (
    SELECT
        fp.track_id,
        fp.album_id,
        cp.period,
        COUNT(*) AS play_count,
        CAST(SUM(t.duration_ms) / 60000 AS INT64) AS listening_time_min,
        MAX(fp.played_at) AS last_played_at
    FROM {{ ref('svc_hub__fact_played') }} AS fp
    LEFT JOIN {{ ref('svc_hub__ref_track') }} AS t ON fp.track_id = t.track_id
    CROSS JOIN {{ ref('stg_hub__ref_calendar') }} AS cp
    WHERE
        (cp.period_start IS NULL OR DATE(fp.played_at, 'Europe/Paris') >= cp.period_start)
        AND DATE(fp.played_at, 'Europe/Paris') <= cp.period_end
    GROUP BY fp.track_id, fp.album_id, cp.period
),

prev_agg AS (
    SELECT
        fp.track_id,
        cp.period,
        CAST(SUM(t.duration_ms) / 60000 AS INT64) AS listening_time_min,
        MAX(fp.played_at) AS last_played_at
    FROM {{ ref('svc_hub__fact_played') }} AS fp
    LEFT JOIN {{ ref('svc_hub__ref_track') }} AS t ON fp.track_id = t.track_id
    CROSS JOIN {{ ref('stg_hub__ref_calendar') }} AS cp
    WHERE
        cp.prev_period_start IS NOT NULL
        AND DATE(fp.played_at, 'Europe/Paris') >= cp.prev_period_start
        AND DATE(fp.played_at, 'Europe/Paris') <= cp.prev_period_end
    GROUP BY fp.track_id, cp.period
),

current_ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY period ORDER BY listening_time_min DESC) AS rank
    FROM current_agg
),

prev_ranked AS (
    SELECT
        track_id,
        period,
        ROW_NUMBER() OVER (PARTITION BY period ORDER BY listening_time_min DESC) AS rank_previous
    FROM prev_agg
),

primary_artist AS (
    SELECT
        bta.track_id,
        a.artist_name
    FROM {{ ref('svc_hub__bridge_track_artist') }} AS bta
    INNER JOIN {{ ref('svc_hub__ref_artist') }} AS a ON bta.artist_id = a.artist_id
    WHERE bta.artist_position = 0
)

SELECT
    c.period,
    c.rank,
    pr.rank_previous,
    c.track_id,
    t.track_name,
    pa.artist_name,
    c.album_id,
    al.album_name,
    al.album_image_url,
    c.play_count,
    c.listening_time_min,
    t.duration_ms,
    c.last_played_at,
    (pr.rank_previous IS NULL AND c.period != 'all') AS is_new_entry
FROM current_ranked AS c
LEFT JOIN prev_ranked AS pr
    ON c.track_id = pr.track_id AND c.period = pr.period
LEFT JOIN {{ ref('svc_hub__ref_track') }} AS t ON c.track_id = t.track_id
LEFT JOIN {{ ref('svc_hub__ref_album') }} AS al ON c.album_id = al.album_id
LEFT JOIN primary_artist AS pa ON c.track_id = pa.track_id
WHERE c.rank <= 20
ORDER BY c.period, c.rank
