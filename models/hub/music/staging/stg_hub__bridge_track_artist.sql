{{
    config(
        materialized='view',
        tags=['spotify']
    )
}}

WITH from_album_tracks AS (
    SELECT
        track_id,
        artist.id AS artist_id,
        artist_position,
        _ingested_at
    FROM {{ ref('svc_spotify__album_tracks') }},
        UNNEST(artists) AS artist WITH OFFSET AS artist_position
),

from_top_tracks AS (
    SELECT
        track_id,
        artist.id AS artist_id,
        artist_position,
        _ingested_at
    FROM {{ ref('svc_spotify__top_tracks') }},
        UNNEST(artists) AS artist WITH OFFSET AS artist_position
),

from_recently_played AS (
    SELECT
        track.id AS track_id,
        artist.id AS artist_id,
        artist_position,
        _ingested_at
    FROM {{ ref('svc_spotify__recently_played') }},
        UNNEST(track.artists) AS artist WITH OFFSET AS artist_position
),

combined AS (
    SELECT * FROM from_album_tracks
    UNION ALL
    SELECT * FROM from_top_tracks
    UNION ALL
    SELECT * FROM from_recently_played
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY track_id, artist_id
            ORDER BY _ingested_at DESC
        ) AS _row_number
    FROM combined
)

SELECT * EXCEPT (_row_number)
FROM deduplicated
WHERE _row_number = 1
