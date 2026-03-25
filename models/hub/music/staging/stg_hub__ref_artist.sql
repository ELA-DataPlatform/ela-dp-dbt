{{
    config(
        materialized='view',
        tags=['spotify']
    )
}}

WITH new_track_artists AS (
    SELECT
        artist.id AS artist_id,
        artist.name AS artist_name,
        artist.uri AS artist_uri,
        _ingested_at
    FROM {{ ref('svc_spotify__recently_played') }},
        UNNEST(track.artists) AS artist
),

new_album_artists AS (
    SELECT
        artist.id AS artist_id,
        artist.name AS artist_name,
        artist.uri AS artist_uri,
        _ingested_at
    FROM {{ ref('svc_spotify__recently_played') }},
        UNNEST(track.album.artists) AS artist
),

legacy_track_artists AS (
    SELECT
        artist.id AS artist_id,
        artist.name AS artist_name,
        artist.uri AS artist_uri,
        _ingested_at
    FROM {{ ref('svc_spotify_legacy__recently_played') }},
        UNNEST(track.artists) AS artist
),

legacy_album_artists AS (
    SELECT
        artist.id AS artist_id,
        artist.name AS artist_name,
        artist.uri AS artist_uri,
        _ingested_at
    FROM {{ ref('svc_spotify_legacy__recently_played') }},
        UNNEST(track.album.artists) AS artist
),

all_artists AS (
    SELECT * FROM new_track_artists
    UNION ALL
    SELECT * FROM new_album_artists
    UNION ALL
    SELECT * FROM legacy_track_artists
    UNION ALL
    SELECT * FROM legacy_album_artists
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY artist_id
            ORDER BY _ingested_at DESC
        ) AS _row_number
    FROM all_artists
)

SELECT * EXCEPT (_row_number)
FROM deduplicated
WHERE _row_number = 1
