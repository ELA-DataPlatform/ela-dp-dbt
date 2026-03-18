{{
    config(
        materialized='view',
        tags=['spotify']
    )
}}

WITH source AS (
    SELECT
        played_at,
        track_id,
        track.album.id AS album_id,
        context.type AS context_type,
        context.uri AS context_uri,
        _ingested_at
    FROM {{ ref('svc_spotify__recently_played') }}
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY played_at, track_id
            ORDER BY _ingested_at DESC
        ) AS _row_number
    FROM source
)

SELECT * EXCEPT (_row_number)
FROM deduplicated
WHERE _row_number = 1
