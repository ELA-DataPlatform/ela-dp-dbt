{{
    config(
        materialized='view',
        tags=['spotify']
    )
}}

WITH from_recently_played_track_artists AS (
    SELECT artist.id AS artist_id
    FROM {{ ref('svc_spotify__recently_played') }},
        UNNEST(track.artists) AS artist
),

from_recently_played_album_artists AS (
    SELECT artist.id AS artist_id
    FROM {{ ref('svc_spotify__recently_played') }},
        UNNEST(track.album.artists) AS artist
),

from_saved_tracks_track_artists AS (
    SELECT artist.id AS artist_id
    FROM {{ ref('svc_spotify__saved_tracks') }},
        UNNEST(track.artists) AS artist
),

from_saved_tracks_album_artists AS (
    SELECT artist.id AS artist_id
    FROM {{ ref('svc_spotify__saved_tracks') }},
        UNNEST(track.album.artists) AS artist
),

from_saved_albums_artists AS (
    SELECT artist.id AS artist_id
    FROM {{ ref('svc_spotify__saved_albums') }},
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
)

SELECT DISTINCT artist_id
FROM all_artist_ids
