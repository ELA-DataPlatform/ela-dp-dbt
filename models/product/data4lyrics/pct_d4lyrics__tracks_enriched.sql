{{
    config(
        materialized='view',
        tags=['spotify']
    )
}}

WITH tracks AS (
    SELECT
        track_id,
        track_name,
        duration_ms,
        album_id
    FROM {{ ref('svc_hub__ref_track') }}
    WHERE track_id NOT IN (SELECT track_id FROM {{ ref('svc_lyrics__lyrics')}})
),

albums AS (
    SELECT
        album_id,
        album_name
    FROM {{ ref('svc_hub__ref_album') }}
),

main_artists AS (
    SELECT
        bta.track_id,
        a.artist_name
    FROM {{ ref('svc_hub__bridge_track_artist') }} bta
    INNER JOIN {{ ref('svc_hub__ref_artist') }} a
        ON bta.artist_id = a.artist_id
    WHERE bta.artist_position = 0
)

SELECT
    t.track_id,
    ma.artist_name AS main_artist,
    al.album_name,
    t.track_name,
    ROUND(t.duration_ms / 1000.0, 0) AS duration_seconds
FROM tracks t
LEFT JOIN albums al
    ON t.album_id = al.album_id
LEFT JOIN main_artists ma
    ON t.track_id = ma.track_id
LIMIT 1000