{{
    config(
        materialized='table',
        tags=['spotify']
    )
}}

WITH fact_with_album AS (
    SELECT
        fp.played_at,
        fp.track_id,
        fp.album_id,
        t.duration_ms
    FROM {{ ref('svc_hub__fact_played') }} AS fp
    LEFT JOIN {{ ref('svc_hub__ref_track') }} AS t ON fp.track_id = t.track_id
    WHERE fp.album_id IS NOT NULL
),

current_agg AS (
    SELECT
        album_id,
        COUNT(*) AS play_count,
        CAST(SUM(duration_ms) / 60000 AS INT64) AS listening_time_min,
        COUNT(DISTINCT track_id) AS unique_tracks,
        MAX(played_at) AS last_played_at,
        '7d' AS period
    FROM fact_with_album
    WHERE played_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
    GROUP BY album_id

    UNION ALL

    SELECT
        album_id,
        COUNT(*) AS play_count,
        CAST(SUM(duration_ms) / 60000 AS INT64) AS listening_time_min,
        COUNT(DISTINCT track_id) AS unique_tracks,
        MAX(played_at) AS last_played_at,
        '30d' AS period
    FROM fact_with_album
    WHERE played_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
    GROUP BY album_id

    UNION ALL

    SELECT
        album_id,
        COUNT(*) AS play_count,
        CAST(SUM(duration_ms) / 60000 AS INT64) AS listening_time_min,
        COUNT(DISTINCT track_id) AS unique_tracks,
        MAX(played_at) AS last_played_at,
        '6m' AS period
    FROM fact_with_album
    WHERE played_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 180 DAY)
    GROUP BY album_id

    UNION ALL

    SELECT
        album_id,
        COUNT(*) AS play_count,
        CAST(SUM(duration_ms) / 60000 AS INT64) AS listening_time_min,
        COUNT(DISTINCT track_id) AS unique_tracks,
        MAX(played_at) AS last_played_at,
        'all' AS period
    FROM fact_with_album
    GROUP BY album_id
),

prev_agg AS (
    SELECT
        album_id,
        CAST(SUM(duration_ms) / 60000 AS INT64) AS listening_time_min,
        '7d' AS period
    FROM fact_with_album
    WHERE played_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)
        AND played_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
    GROUP BY album_id

    UNION ALL

    SELECT
        album_id,
        CAST(SUM(duration_ms) / 60000 AS INT64) AS listening_time_min,
        '30d' AS period
    FROM fact_with_album
    WHERE played_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 60 DAY)
        AND played_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
    GROUP BY album_id

    UNION ALL

    SELECT
        album_id,
        CAST(SUM(duration_ms) / 60000 AS INT64) AS listening_time_min,
        '6m' AS period
    FROM fact_with_album
    WHERE played_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 360 DAY)
        AND played_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 180 DAY)
    GROUP BY album_id
),

current_ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY period ORDER BY listening_time_min DESC) AS rank
    FROM current_agg
),

prev_ranked AS (
    SELECT
        album_id,
        period,
        ROW_NUMBER() OVER (PARTITION BY period ORDER BY listening_time_min DESC) AS rank_previous
    FROM prev_agg
),

primary_artist AS (
    SELECT
        baa.album_id,
        a.artist_id,
        a.artist_name
    FROM {{ ref('svc_hub__bridge_album_artist') }} AS baa
    INNER JOIN {{ ref('svc_hub__ref_artist') }} AS a ON baa.artist_id = a.artist_id
    WHERE baa.artist_position = 0
),

album_images AS (
    SELECT
        album_id,
        JSON_VALUE(JSON_QUERY(images, '$[0]'), '$.url') AS album_image_url
    FROM {{ ref('svc_spotify__album_detail') }}
    WHERE images IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY album_id ORDER BY _ingested_at DESC) = 1
)

SELECT
    c.period,
    c.rank,
    pr.rank_previous,
    c.album_id,
    al.album_name,
    pa.artist_id,
    pa.artist_name,
    ai.album_image_url,
    c.play_count,
    c.listening_time_min,
    c.unique_tracks,
    c.last_played_at,
    (pr.rank_previous IS NULL AND c.period != 'all') AS is_new_entry,
    CAST(SUBSTR(al.release_date, 1, 4) AS INT64) AS release_year,
    al.total_tracks
FROM current_ranked AS c
LEFT JOIN prev_ranked AS pr
    ON c.album_id = pr.album_id AND c.period = pr.period
LEFT JOIN {{ ref('svc_hub__ref_album') }} AS al ON c.album_id = al.album_id
LEFT JOIN primary_artist AS pa ON c.album_id = pa.album_id
LEFT JOIN album_images AS ai ON c.album_id = ai.album_id
WHERE c.rank <= 20
ORDER BY c.period, c.rank
