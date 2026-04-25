{{
    config(
        materialized='view',
        tags=['spotify']
    )
}}

WITH from_recently_played AS (
    SELECT JSON_VALUE(track, '$.album.id') AS album_id
    FROM {{ ref('svc_spotify__recently_played') }}
),

from_saved_tracks AS (
    SELECT JSON_VALUE(track, '$.album.id') AS album_id
    FROM {{ ref('svc_spotify__saved_tracks') }}
),

from_saved_albums AS (
    SELECT album_id
    FROM {{ ref('svc_spotify__saved_albums') }}
),

from_legacy_recently_played AS (
    SELECT track.album.id AS album_id
    FROM {{ ref('svc_spotify_legacy__recently_played') }}
),

from_legacy_saved_tracks AS (
    SELECT track.album.id AS album_id
    FROM {{ ref('svc_spotify_legacy__saved_tracks') }}
),

from_legacy_saved_albums AS (
    SELECT album_id
    FROM {{ ref('svc_spotify_legacy__saved_albums') }}
),

all_album_ids AS (
    SELECT album_id FROM from_recently_played
    UNION ALL
    SELECT album_id FROM from_saved_tracks
    UNION ALL
    SELECT album_id FROM from_saved_albums
    UNION ALL
    SELECT album_id FROM from_legacy_recently_played
    UNION ALL
    SELECT album_id FROM from_legacy_saved_tracks
    UNION ALL
    SELECT album_id FROM from_legacy_saved_albums
)

SELECT DISTINCT album_id
FROM all_album_ids
