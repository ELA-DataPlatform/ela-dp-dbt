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
        JSON_VALUE(external_ids, '$.isrc') AS isrc,
        JSON_VALUE(album, '$.id') AS album_id,
        is_local,
        _ingested_at,
        0 AS _source_priority
    FROM {{ ref('svc_spotify__top_tracks') }}
),

from_album_tracks AS (
    SELECT
        track_id,
        name AS track_name,
        uri AS track_uri,
        duration_ms,
        explicit,
        track_number,
        disc_number,
        CAST(NULL AS INT64) AS popularity,
        CAST(NULL AS STRING) AS isrc,
        album_id,
        is_local,
        _ingested_at,
        1 AS _source_priority
    FROM {{ ref('svc_spotify__album_tracks') }}
),

from_recently_played AS (
    SELECT
        track_id,
        JSON_VALUE(track, '$.name') AS track_name,
        JSON_VALUE(track, '$.uri') AS track_uri,
        CAST(JSON_VALUE(track, '$.duration_ms') AS INT64) AS duration_ms,
        CAST(JSON_VALUE(track, '$.explicit') AS BOOL) AS explicit,
        CAST(JSON_VALUE(track, '$.track_number') AS INT64) AS track_number,
        CAST(JSON_VALUE(track, '$.disc_number') AS INT64) AS disc_number,
        CAST(JSON_VALUE(track, '$.popularity') AS INT64) AS popularity,
        JSON_VALUE(track, '$.external_ids.isrc') AS isrc,
        JSON_VALUE(track, '$.album.id') AS album_id,
        CAST(JSON_VALUE(track, '$.is_local') AS BOOL) AS is_local,
        _ingested_at,
        2 AS _source_priority
    FROM {{ ref('svc_spotify__recently_played') }}
),

from_legacy_top_tracks AS (
    SELECT
        id AS track_id,
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
    FROM {{ ref('svc_spotify_legacy__top_tracks') }}
),

from_legacy_album_tracks AS (
    SELECT
        id AS track_id,
        name AS track_name,
        uri AS track_uri,
        duration_ms,
        explicit,
        track_number,
        disc_number,
        CAST(NULL AS INT64) AS popularity,
        CAST(NULL AS STRING) AS isrc,
        album_id,
        is_local,
        _ingested_at,
        1 AS _source_priority
    FROM {{ ref('svc_spotify_legacy__album_tracks') }}
),

from_legacy_recently_played AS (
    SELECT
        track_id,
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
        2 AS _source_priority
    FROM {{ ref('svc_spotify_legacy__recently_played') }}
),

combined AS (
    SELECT * FROM from_top_tracks
    UNION ALL
    SELECT * FROM from_album_tracks
    UNION ALL
    SELECT * FROM from_recently_played
    UNION ALL
    SELECT * FROM from_legacy_top_tracks
    UNION ALL
    SELECT * FROM from_legacy_album_tracks
    UNION ALL
    SELECT * FROM from_legacy_recently_played
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
