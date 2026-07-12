{{
    config(
        materialized='table',
        tags=['spotify', 'webapp'],
        cluster_by=['artist_id']
    )
}}

-- One row per (artist_id, album_id) for the album's primary artist
-- (bridge_album_artist.artist_position = 0). Restricted to album_type = 'album'
-- (no singles, no compilations). Covers the full KNOWN discography including
-- albums with no plays yet (listening metrics default to 0).

WITH album_plays AS (
    SELECT
        fp.album_id,
        COUNT(*) AS plays,
        {{ milliseconds_to_minutes('SUM(COALESCE(t.duration_ms, 0))') }} AS listening_time_min,
        COUNT(DISTINCT fp.track_id) AS listened_tracks
    FROM {{ ref('svc_hub__fact_played') }} AS fp
    LEFT JOIN {{ ref('svc_hub__ref_track') }} AS t ON fp.track_id = t.track_id
    WHERE fp.album_id IS NOT NULL
    GROUP BY fp.album_id
),

artist_albums AS (
    SELECT
        baa.artist_id,
        al.album_id,
        al.album_name,
        al.album_image_url,
        al.total_tracks,
        al.release_date
    FROM {{ ref('svc_hub__bridge_album_artist') }} AS baa
    INNER JOIN {{ ref('svc_hub__ref_album') }} AS al ON baa.album_id = al.album_id
    WHERE
        baa.artist_position = 0
        AND al.album_type = 'album'
)

SELECT
    aa.artist_id,
    aa.album_id,
    aa.album_name,
    aa.album_image_url,
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
