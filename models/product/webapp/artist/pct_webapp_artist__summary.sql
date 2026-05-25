{{
    config(
        materialized='table',
        tags=['spotify', 'webapp'],
        cluster_by=['artist_id']
    )
}}

-- One row per listened artist. Feeds the hero card, the all-time rank and the
-- artist selector. Plays are credited to the primary artist of each track
-- (bridge_track_artist.artist_position = 0). The source has no per-stream
-- ms_played, so listening_time_min approximates time with full track duration
-- (SUM(duration_ms) / 60000), consistent with the existing ranking marts.

WITH track_plays AS (
    SELECT
        bta.artist_id,
        fp.track_id,
        fp.played_at,
        COALESCE(t.duration_ms, 0) AS duration_ms
    FROM {{ ref('svc_hub__fact_played') }} AS fp
    INNER JOIN {{ ref('svc_hub__bridge_track_artist') }} AS bta
        ON fp.track_id = bta.track_id AND bta.artist_position = 0
    LEFT JOIN {{ ref('svc_hub__ref_track') }} AS t ON fp.track_id = t.track_id
),

artist_totals AS (
    SELECT
        artist_id,
        MIN(played_at) AS first_listened_at,
        MAX(played_at) AS last_listened_at,
        COUNT(*) AS total_plays,
        CAST(SUM(duration_ms) / 60000 AS INT64) AS total_listening_time_min,
        COUNT(DISTINCT track_id) AS distinct_tracks_listened
    FROM track_plays
    GROUP BY artist_id
),

-- Distinct tracks listened per album_id, used to assess full-album completion.
album_listened AS (
    SELECT
        album_id,
        COUNT(DISTINCT track_id) AS listened_tracks
    FROM {{ ref('svc_hub__fact_played') }}
    WHERE album_id IS NOT NULL
    GROUP BY album_id
),

-- Known studio discography per artist (primary album artist), with completion.
artist_albums AS (
    SELECT
        baa.artist_id,
        al.album_id,
        COALESCE(ov.is_studio, al.album_type = 'album') AS is_studio_album,
        COALESCE(
            COALESCE(albl.listened_tracks, 0) >= al.total_tracks AND al.total_tracks > 0,
            FALSE
        ) AS is_complete
    FROM {{ ref('svc_hub__bridge_album_artist') }} AS baa
    INNER JOIN {{ ref('svc_hub__ref_album') }} AS al ON baa.album_id = al.album_id
    LEFT JOIN album_listened AS albl ON al.album_id = albl.album_id
    LEFT JOIN {{ ref('seed_album_studio_overrides') }} AS ov ON al.album_id = ov.album_id
    WHERE baa.artist_position = 0
),

artist_studio AS (
    SELECT
        artist_id,
        COUNTIF(is_studio_album) AS studio_albums_total,
        COUNTIF(is_studio_album AND is_complete) AS studio_albums_completed
    FROM artist_albums
    GROUP BY artist_id
),

combined AS (
    SELECT
        att.artist_id,
        att.first_listened_at,
        att.last_listened_at,
        att.total_plays,
        att.total_listening_time_min,
        att.distinct_tracks_listened,
        COALESCE(asg.studio_albums_total, 0) AS studio_albums_total,
        COALESCE(asg.studio_albums_completed, 0) AS studio_albums_completed
    FROM artist_totals AS att
    LEFT JOIN artist_studio AS asg ON att.artist_id = asg.artist_id
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
    c.artist_id,
    a.artist_name,
    ai.artist_image_url,
    c.first_listened_at,
    c.total_listening_time_min,
    c.total_plays,
    c.distinct_tracks_listened,
    c.studio_albums_total,
    c.studio_albums_completed,
    c.last_listened_at,
    JSON_VALUE_ARRAY(a.genres) AS genres,
    DATE(c.first_listened_at, 'Europe/Paris') AS first_listened_date,
    EXTRACT(YEAR FROM DATE(c.first_listened_at, 'Europe/Paris')) AS first_listened_year,
    RANK() OVER (
        ORDER BY c.total_listening_time_min DESC, c.total_plays DESC, c.artist_id ASC
    ) AS all_time_rank
FROM combined AS c
LEFT JOIN {{ ref('svc_hub__ref_artist') }} AS a ON c.artist_id = a.artist_id
LEFT JOIN artist_images AS ai ON c.artist_id = ai.artist_id
