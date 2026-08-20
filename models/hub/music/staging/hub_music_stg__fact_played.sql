{{
    config(
        materialized='view',
        tags=['spotify']
    )
}}

WITH from_recently_played AS (
    SELECT
        played_at,
        track_id,
        JSON_VALUE(track, '$.album.id') AS album_id,
        JSON_VALUE(context, '$.type') AS context_type,
        JSON_VALUE(context, '$.uri') AS context_uri,
        _ingested_at
    FROM {{ ref('dlk_spotify_svc__recently_played') }}
),

from_legacy_recently_played AS (
    SELECT
        played_at,
        track_id,
        track.album.id AS album_id,
        context.type AS context_type,
        context.uri AS context_uri,
        _ingested_at
    FROM {{ ref('dlk_spotify_legacy_svc__recently_played') }}
),

combined AS (
    SELECT * FROM from_recently_played
    UNION ALL
    SELECT * FROM from_legacy_recently_played
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY played_at, track_id
            ORDER BY _ingested_at DESC
        ) AS _row_number
    FROM combined
)

SELECT * EXCEPT (_row_number)
FROM deduplicated
WHERE _row_number = 1
