{{
    config(
        materialized='table',
        tags=['spotify', 'webapp']
    )
}}

WITH

period_bounds AS (
    SELECT
        period_start,
        period_end,
        prev_period_start,
        prev_period_end
    FROM {{ ref('stg_hub__ref_calendar') }}
    WHERE period = '7d'
),

fact_with_duration AS (
    SELECT
        fp.track_id,
        fp.album_id,
        date(fp.played_at, 'Europe/Paris') AS play_date,
        coalesce(t.duration_ms, 0) AS duration_ms
    FROM {{ ref('svc_hub__fact_played') }} AS fp
    LEFT JOIN {{ ref('svc_hub__ref_track') }} AS t ON fp.track_id = t.track_id
    CROSS JOIN period_bounds AS pb
    WHERE
        date(fp.played_at, 'Europe/Paris') >= pb.prev_period_start
        AND date(fp.played_at, 'Europe/Paris') <= pb.period_end
),

-- ── Precomputed images ───────────────────────────────────────────────────────

artist_images AS (
    SELECT
        artist_id,
        json_value(json_query(images, '$[0]'), '$.url') AS image_url
    FROM {{ ref('svc_spotify__artist_detail') }}
    WHERE images IS NOT NULL
    QUALIFY row_number() OVER (PARTITION BY artist_id ORDER BY _ingested_at DESC) = 1
),

album_images AS (
    SELECT
        album_id,
        json_value(json_query(images, '$[0]'), '$.url') AS image_url
    FROM {{ ref('svc_spotify__album_detail') }}
    WHERE images IS NOT NULL
    QUALIFY row_number() OVER (PARTITION BY album_id ORDER BY _ingested_at DESC) = 1
),

-- ── ARTIST ───────────────────────────────────────────────────────────────────

artist_plays AS (
    SELECT
        fw.play_date,
        fw.duration_ms,
        bta.artist_id
    FROM fact_with_duration AS fw
    INNER JOIN {{ ref('svc_hub__bridge_track_artist') }} AS bta
        ON fw.track_id = bta.track_id AND bta.artist_position = 0
),

artist_current AS (
    SELECT
        artist_id,
        cast(sum(duration_ms) / 60000 AS int64) AS listening_minutes
    FROM artist_plays
    CROSS JOIN period_bounds AS pb
    WHERE play_date >= pb.period_start AND play_date <= pb.period_end
    GROUP BY artist_id
),

artist_prev AS (
    SELECT
        artist_id,
        cast(sum(duration_ms) / 60000 AS int64) AS listening_minutes
    FROM artist_plays
    CROSS JOIN period_bounds AS pb
    WHERE play_date >= pb.prev_period_start AND play_date <= pb.prev_period_end
    GROUP BY artist_id
),

artist_current_ranked AS (
    SELECT
        artist_id,
        listening_minutes,
        row_number() OVER (ORDER BY listening_minutes DESC) AS rank
    FROM artist_current
),

artist_prev_ranked AS (
    SELECT
        artist_id,
        row_number() OVER (ORDER BY listening_minutes DESC) AS rank_prev
    FROM artist_prev
),

artists_top5 AS (
    SELECT
        'artist' AS ranking_type,
        c.rank,
        a.artist_name AS entity_name,
        cast(NULL AS string) AS subtitle,
        c.listening_minutes,
        ai.image_url,
        CASE
            WHEN p.rank_prev IS NULL THEN 'new'
            WHEN p.rank_prev - c.rank > 0 THEN concat('+', cast(p.rank_prev - c.rank AS string))
            WHEN p.rank_prev - c.rank < 0 THEN cast(p.rank_prev - c.rank AS string)
            ELSE '0'
        END AS rank_change
    FROM artist_current_ranked AS c
    LEFT JOIN artist_prev_ranked AS p ON c.artist_id = p.artist_id
    LEFT JOIN {{ ref('svc_hub__ref_artist') }} AS a ON c.artist_id = a.artist_id
    LEFT JOIN artist_images AS ai ON c.artist_id = ai.artist_id
    WHERE c.rank <= 5
),

-- ── ALBUM ────────────────────────────────────────────────────────────────────

album_current AS (
    SELECT
        album_id,
        cast(sum(duration_ms) / 60000 AS int64) AS listening_minutes
    FROM fact_with_duration
    CROSS JOIN period_bounds AS pb
    WHERE play_date >= pb.period_start AND play_date <= pb.period_end
    GROUP BY album_id
),

album_prev AS (
    SELECT
        album_id,
        cast(sum(duration_ms) / 60000 AS int64) AS listening_minutes
    FROM fact_with_duration
    CROSS JOIN period_bounds AS pb
    WHERE play_date >= pb.prev_period_start AND play_date <= pb.prev_period_end
    GROUP BY album_id
),

album_current_ranked AS (
    SELECT
        album_id,
        listening_minutes,
        row_number() OVER (ORDER BY listening_minutes DESC) AS rank
    FROM album_current
),

album_prev_ranked AS (
    SELECT
        album_id,
        row_number() OVER (ORDER BY listening_minutes DESC) AS rank_prev
    FROM album_prev
),

album_primary_artist AS (
    SELECT
        baa.album_id,
        a.artist_name
    FROM {{ ref('svc_hub__bridge_album_artist') }} AS baa
    INNER JOIN {{ ref('svc_hub__ref_artist') }} AS a ON baa.artist_id = a.artist_id
    WHERE baa.artist_position = 0
),

albums_top5 AS (
    SELECT
        'album' AS ranking_type,
        c.rank,
        alb.album_name AS entity_name,
        pa.artist_name AS subtitle,
        c.listening_minutes,
        ai.image_url,
        CASE
            WHEN p.rank_prev IS NULL THEN 'new'
            WHEN p.rank_prev - c.rank > 0 THEN concat('+', cast(p.rank_prev - c.rank AS string))
            WHEN p.rank_prev - c.rank < 0 THEN cast(p.rank_prev - c.rank AS string)
            ELSE '0'
        END AS rank_change
    FROM album_current_ranked AS c
    LEFT JOIN album_prev_ranked AS p ON c.album_id = p.album_id
    LEFT JOIN {{ ref('svc_hub__ref_album') }} AS alb ON c.album_id = alb.album_id
    LEFT JOIN album_primary_artist AS pa ON c.album_id = pa.album_id
    LEFT JOIN album_images AS ai ON c.album_id = ai.album_id
    WHERE c.rank <= 5
),

-- ── TRACK ────────────────────────────────────────────────────────────────────

track_current AS (
    SELECT
        track_id,
        cast(sum(duration_ms) / 60000 AS int64) AS listening_minutes,
        max(album_id) AS album_id
    FROM fact_with_duration
    CROSS JOIN period_bounds AS pb
    WHERE play_date >= pb.period_start AND play_date <= pb.period_end
    GROUP BY track_id
),

track_prev AS (
    SELECT
        track_id,
        cast(sum(duration_ms) / 60000 AS int64) AS listening_minutes
    FROM fact_with_duration
    CROSS JOIN period_bounds AS pb
    WHERE play_date >= pb.prev_period_start AND play_date <= pb.prev_period_end
    GROUP BY track_id
),

track_current_ranked AS (
    SELECT
        track_id,
        album_id,
        listening_minutes,
        row_number() OVER (ORDER BY listening_minutes DESC) AS rank
    FROM track_current
),

track_prev_ranked AS (
    SELECT
        track_id,
        row_number() OVER (ORDER BY listening_minutes DESC) AS rank_prev
    FROM track_prev
),

track_primary_artist AS (
    SELECT
        bta.track_id,
        a.artist_name
    FROM {{ ref('svc_hub__bridge_track_artist') }} AS bta
    INNER JOIN {{ ref('svc_hub__ref_artist') }} AS a ON bta.artist_id = a.artist_id
    WHERE bta.artist_position = 0
),

tracks_top5 AS (
    SELECT
        'track' AS ranking_type,
        c.rank,
        t.track_name AS entity_name,
        pa.artist_name AS subtitle,
        c.listening_minutes,
        ai.image_url,
        CASE
            WHEN p.rank_prev IS NULL THEN 'new'
            WHEN p.rank_prev - c.rank > 0 THEN concat('+', cast(p.rank_prev - c.rank AS string))
            WHEN p.rank_prev - c.rank < 0 THEN cast(p.rank_prev - c.rank AS string)
            ELSE '0'
        END AS rank_change
    FROM track_current_ranked AS c
    LEFT JOIN track_prev_ranked AS p ON c.track_id = p.track_id
    LEFT JOIN {{ ref('svc_hub__ref_track') }} AS t ON c.track_id = t.track_id
    LEFT JOIN track_primary_artist AS pa ON c.track_id = pa.track_id
    LEFT JOIN album_images AS ai ON c.album_id = ai.album_id
    WHERE c.rank <= 5
)

-- ── FINAL ────────────────────────────────────────────────────────────────────

SELECT
    date_sub(current_date('Europe/Paris'), INTERVAL 6 DAY) AS period_start,
    current_date('Europe/Paris') AS period_end,

    array(
        SELECT AS STRUCT
            rank,
            entity_name,
            subtitle,
            listening_minutes,
            image_url,
            rank_change,
            {{ minutes_to_label('listening_minutes') }} AS listening_label
        FROM artists_top5
        ORDER BY rank
    ) AS artists,

    array(
        SELECT AS STRUCT
            rank,
            entity_name,
            subtitle,
            listening_minutes,
            image_url,
            rank_change,
            {{ minutes_to_label('listening_minutes') }} AS listening_label
        FROM albums_top5
        ORDER BY rank
    ) AS albums,

    array(
        SELECT AS STRUCT
            rank,
            entity_name,
            subtitle,
            listening_minutes,
            image_url,
            rank_change,
            {{ minutes_to_label('listening_minutes') }} AS listening_label
        FROM tracks_top5
        ORDER BY rank
    ) AS tracks
