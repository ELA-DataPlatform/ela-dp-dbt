{{
    config(
        materialized='view',
        tags=['spotify']
    )
}}

WITH from_new AS (
    SELECT
        played_at,
        track_id,
        track.album.id AS album_id,
        context.type AS context_type,
        context.uri AS context_uri,
        _ingested_at
    FROM {{ ref('svc_spotify__recently_played') }}
),

from_legacy AS (
    SELECT
        played_at,
        track_id,
        track.album.id AS album_id,
        context.type AS context_type,
        context.uri AS context_uri,
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
            PARTITION BY played_at, track_id
            ORDER BY _ingested_at DESC
        ) AS _row_number
    FROM unioned
)

SELECT * EXCEPT (_row_number)
FROM deduplicated
WHERE _row_number = 1
