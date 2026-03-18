{{
    config(
        materialized='view',
        tags=['spotify']
    )
}}

WITH source AS (
    SELECT
        track.album.id AS album_id,
        track.album.name AS album_name,
        track.album.album_type AS album_type,
        track.album.total_tracks AS total_tracks,
        track.album.release_date AS release_date,
        track.album.release_date_precision AS release_date_precision,
        track.album.uri AS album_uri,
        _ingested_at
    FROM {{ ref('svc_spotify__recently_played') }}
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY album_id
            ORDER BY _ingested_at DESC
        ) AS _row_number
    FROM source
)

SELECT * EXCEPT (_row_number)
FROM deduplicated
WHERE _row_number = 1
