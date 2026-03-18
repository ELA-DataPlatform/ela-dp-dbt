{{
    config(
        materialized='view',
        tags=['spotify']
    )
}}

WITH track_artists AS (
    SELECT
        artist.id AS artist_id,
        artist.name AS artist_name,
        artist.uri AS artist_uri,
        _ingested_at
    FROM {{ ref('svc_spotify__recently_played') }},
    UNNEST(track.artists) AS artist
),

album_artists AS (
    SELECT
        artist.id AS artist_id,
        artist.name AS artist_name,
        artist.uri AS artist_uri,
        _ingested_at
    FROM {{ ref('svc_spotify__recently_played') }},
    UNNEST(track.album.artists) AS artist
),

all_artists AS (
    SELECT * FROM track_artists
    UNION ALL
    SELECT * FROM album_artists
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
