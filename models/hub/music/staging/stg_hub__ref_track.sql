{{
    config(
        materialized='view',
        tags=['spotify']
    )
}}

WITH from_top_tracks AS (
    SELECT
        track_id,
        name AS track_name,
        uri AS track_uri,
        duration_ms,
        explicit,
        track_number,
        disc_number,
        popularity,
        external_ids.isrc,
        album.id AS album_id,
        is_local,
        _ingested_at,
        0 AS _source_priority
    FROM {{ ref('svc_spotify__top_tracks') }}
),

from_recently_played AS (
    SELECT
        track.id AS track_id,
        track.name AS track_name,
        track.uri AS track_uri,
        track.duration_ms,
        track.explicit,
        track.track_number,
        track.disc_number,
        track.popularity,
        track.external_ids.isrc,
        track.album.id AS album_id,
        track.is_local,
        _ingested_at,
        1 AS _source_priority
    FROM {{ ref('svc_spotify__recently_played') }}
),

combined AS (
    SELECT * FROM from_top_tracks
    UNION ALL
    SELECT * FROM from_recently_played
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY track_id
            ORDER BY _source_priority ASC, _ingested_at DESC
        ) AS _row_number
    FROM combined
)

SELECT * EXCEPT (_row_number, _source_priority)
FROM deduplicated
WHERE _row_number = 1
