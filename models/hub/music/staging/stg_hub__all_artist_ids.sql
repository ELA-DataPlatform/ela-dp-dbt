{{
    config(
        materialized='view',
        tags=['spotify']
    )
}}

WITH from_recently_played_track_artists AS (
    SELECT JSON_VALUE(artist, '$.id') AS artist_id
    FROM {{ ref('svc_spotify__recently_played') }},
        UNNEST(JSON_QUERY_ARRAY(track, '$.artists')) AS artist
),

from_recently_played_album_artists AS (
    SELECT JSON_VALUE(artist, '$.id') AS artist_id
    FROM {{ ref('svc_spotify__recently_played') }},
        UNNEST(JSON_QUERY_ARRAY(track, '$.album.artists')) AS artist
),

from_saved_tracks_track_artists AS (
    SELECT JSON_VALUE(artist, '$.id') AS artist_id
    FROM {{ ref('svc_spotify__saved_tracks') }},
        UNNEST(JSON_QUERY_ARRAY(track, '$.artists')) AS artist
),

from_saved_tracks_album_artists AS (
    SELECT JSON_VALUE(artist, '$.id') AS artist_id
    FROM {{ ref('svc_spotify__saved_tracks') }},
        UNNEST(JSON_QUERY_ARRAY(track, '$.album.artists')) AS artist
),

from_saved_albums_artists AS (
    SELECT JSON_VALUE(artist, '$.id') AS artist_id
    FROM {{ ref('svc_spotify__saved_albums') }},
        UNNEST(JSON_QUERY_ARRAY(album, '$.artists')) AS artist
),

from_legacy_recently_played_track_artists AS (
    SELECT artist.id AS artist_id
    FROM {{ ref('svc_spotify_legacy__recently_played') }},
        UNNEST(track.artists) AS artist
),

from_legacy_recently_played_album_artists AS (
    SELECT artist.id AS artist_id
    FROM {{ ref('svc_spotify_legacy__recently_played') }},
        UNNEST(track.album.artists) AS artist
),

from_legacy_saved_tracks_track_artists AS (
    SELECT artist.id AS artist_id
    FROM {{ ref('svc_spotify_legacy__saved_tracks') }},
        UNNEST(track.artists) AS artist
),

from_legacy_saved_tracks_album_artists AS (
    SELECT artist.id AS artist_id
    FROM {{ ref('svc_spotify_legacy__saved_tracks') }},
        UNNEST(track.album.artists) AS artist
),

from_legacy_saved_albums_artists AS (
    SELECT artist.id AS artist_id
    FROM {{ ref('svc_spotify_legacy__saved_albums') }},
        UNNEST(album.artists) AS artist
),

all_artist_ids AS (
    SELECT artist_id FROM from_recently_played_track_artists
    UNION ALL
    SELECT artist_id FROM from_recently_played_album_artists
    UNION ALL
    SELECT artist_id FROM from_saved_tracks_track_artists
    UNION ALL
    SELECT artist_id FROM from_saved_tracks_album_artists
    UNION ALL
    SELECT artist_id FROM from_saved_albums_artists
    UNION ALL
    SELECT artist_id FROM from_legacy_recently_played_track_artists
    UNION ALL
    SELECT artist_id FROM from_legacy_recently_played_album_artists
    UNION ALL
    SELECT artist_id FROM from_legacy_saved_tracks_track_artists
    UNION ALL
    SELECT artist_id FROM from_legacy_saved_tracks_album_artists
    UNION ALL
    SELECT artist_id FROM from_legacy_saved_albums_artists
)

SELECT DISTINCT artist_id
FROM all_artist_ids
