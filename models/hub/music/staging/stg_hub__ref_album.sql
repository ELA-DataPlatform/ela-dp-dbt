{{
    config(
        materialized='view',
        tags=['spotify']
    )
}}

WITH from_new AS (
    SELECT
        track.album.id AS album_id,
        track.album.name AS album_name,
        track.album.album_type,
        track.album.total_tracks,
        track.album.release_date,
        track.album.release_date_precision,
        track.album.uri AS album_uri,
        _ingested_at
    FROM {{ ref('svc_spotify__recently_played') }}
),

from_legacy AS (
    SELECT
        track.album.id AS album_id,
        track.album.name AS album_name,
        track.album.album_type,
        track.album.total_tracks,
        CAST(track.album.release_date AS STRING) AS release_date,
        track.album.release_date_precision,
        track.album.uri AS album_uri,
        _ingested_at
    FROM {{ ref('svc_spotify_legacy__recently_played') }}
),

unioned AS (
    SELECT * FROM from_new
    UNION ALL
    SELECT * FROM from_legacy
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY album_id
            ORDER BY _ingested_at DESC
        ) AS _row_number
    FROM unioned
)

SELECT * EXCEPT (_row_number)
FROM deduplicated
WHERE _row_number = 1
