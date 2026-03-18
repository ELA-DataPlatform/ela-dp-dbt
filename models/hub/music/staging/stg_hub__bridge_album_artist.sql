{{
    config(
        materialized='view',
        tags=['spotify']
    )
}}

WITH source AS (
    SELECT
        track.album.id AS album_id,
        artist.id AS artist_id,
        artist_position,
        _ingested_at
    FROM {{ ref('svc_spotify__recently_played') }},
        UNNEST(track.album.artists) AS artist WITH OFFSET AS artist_position
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY album_id, artist_id
            ORDER BY _ingested_at DESC
        ) AS _row_number
    FROM source
)

SELECT * EXCEPT (_row_number)
FROM deduplicated
WHERE _row_number = 1
