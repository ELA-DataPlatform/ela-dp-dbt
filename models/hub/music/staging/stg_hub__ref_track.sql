{{
    config(
        materialized='view',
        tags=['spotify']
    )
}}

WITH source AS (
    SELECT
        track.id AS track_id,
        track.name AS track_name,
        track.uri AS track_uri,
        track.duration_ms AS duration_ms,
        track.explicit AS explicit,
        track.track_number AS track_number,
        track.disc_number AS disc_number,
        track.popularity AS popularity,
        track.external_ids.isrc AS isrc,
        track.album.id AS album_id,
        track.is_local AS is_local,
        _ingested_at
    FROM {{ ref('svc_spotify__recently_played') }}
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY track_id
            ORDER BY _ingested_at DESC
        ) AS _row_number
    FROM source
)

SELECT * EXCEPT (_row_number)
FROM deduplicated
WHERE _row_number = 1
