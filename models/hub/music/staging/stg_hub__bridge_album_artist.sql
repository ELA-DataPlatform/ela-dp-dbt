{{
    config(
        materialized='view',
        tags=['spotify']
    )
}}

WITH from_album_detail AS (
    SELECT
        album_id,
        artist.id AS artist_id,
        artist_position,
        _ingested_at
    FROM {{ ref('svc_spotify__album_detail') }},
        UNNEST(artists) AS artist WITH OFFSET AS artist_position
),

from_recently_played AS (
    SELECT
        track.album.id AS album_id,
        artist.id AS artist_id,
        artist_position,
        _ingested_at
    FROM {{ ref('svc_spotify__recently_played') }},
        UNNEST(track.album.artists) AS artist WITH OFFSET AS artist_position
),

combined AS (
    SELECT * FROM from_album_detail
    UNION ALL
    SELECT * FROM from_recently_played
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY album_id, artist_id
            ORDER BY _ingested_at DESC
        ) AS _row_number
    FROM combined
)

SELECT * EXCEPT (_row_number)
FROM deduplicated
WHERE _row_number = 1
