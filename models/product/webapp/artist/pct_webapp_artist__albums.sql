{{
    config(
        materialized='table',
        tags=['spotify', 'webapp'],
        cluster_by=['artist_id']
    )
}}

-- One row per (artist_id, album_id) for the album's primary artist
-- (bridge_album_artist.artist_position = 0). Covers the full KNOWN discography
-- of the artist (every album present in svc_hub__ref_album), including albums
-- with no plays yet (listening metrics default to 0). Feeds the studio gallery
-- (filter is_studio_album) and the per-artist album ranking (order by
-- listening_time_min). listening_time_min approximates time with full track
-- duration; see schema.yml.

WITH album_plays AS (
    SELECT
        fp.album_id,
        COUNT(*) AS plays,
        CAST(SUM(COALESCE(t.duration_ms, 0)) / 60000 AS INT64) AS listening_time_min,
        COUNT(DISTINCT fp.track_id) AS listened_tracks
    FROM {{ ref('svc_hub__fact_played') }} AS fp
    LEFT JOIN {{ ref('svc_hub__ref_track') }} AS t ON fp.track_id = t.track_id
    WHERE fp.album_id IS NOT NULL
    GROUP BY fp.album_id
),

album_images AS (
    SELECT
        album_id,
        JSON_VALUE(JSON_QUERY(images, '$[0]'), '$.url') AS album_image_url
    FROM {{ ref('svc_spotify__album_detail') }}
    WHERE images IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY album_id ORDER BY _ingested_at DESC) = 1
),

artist_albums AS (
    SELECT
        baa.artist_id,
        al.album_id,
        al.album_name,
        al.album_type,
        al.total_tracks,
        al.release_date,
        COALESCE(ov.is_studio, al.album_type = 'album') AS is_studio_album
    FROM {{ ref('svc_hub__bridge_album_artist') }} AS baa
    INNER JOIN {{ ref('svc_hub__ref_album') }} AS al ON baa.album_id = al.album_id
    LEFT JOIN {{ ref('seed_album_studio_overrides') }} AS ov ON al.album_id = ov.album_id
    WHERE baa.artist_position = 0
)

SELECT
    aa.artist_id,
    aa.album_id,
    aa.album_name,
    ai.album_image_url,
    aa.album_type,
    aa.is_studio_album,
    aa.total_tracks,
    SAFE.PARSE_DATE('%Y-%m-%d', aa.release_date) AS release_date,
    SAFE_CAST(SUBSTR(aa.release_date, 1, 4) AS INT64) AS release_year,
    COALESCE(ap.listened_tracks, 0) AS listened_tracks,
    COALESCE(
        COALESCE(ap.listened_tracks, 0) >= aa.total_tracks AND aa.total_tracks > 0,
        FALSE
    ) AS is_complete,
    CASE
        WHEN aa.total_tracks > 0
            THEN LEAST(ROUND(COALESCE(ap.listened_tracks, 0) / aa.total_tracks * 100, 1), 100.0)
        ELSE 0.0
    END AS completion_pct,
    COALESCE(ap.listening_time_min, 0) AS listening_time_min,
    COALESCE(ap.plays, 0) AS plays
FROM artist_albums AS aa
LEFT JOIN album_plays AS ap ON aa.album_id = ap.album_id
LEFT JOIN album_images AS ai ON aa.album_id = ai.album_id
