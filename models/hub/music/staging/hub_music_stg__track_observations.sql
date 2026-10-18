{{ config(materialized='view', tags=['spotify']) }}

-- One row per source observation, not per track. Retains complete artist lists.
WITH observations AS (
    SELECT
        t.track_id,
        TO_JSON_STRING(t) AS track,
        JSON_VALUE(t.album, '$.id') AS album_id,
        t.album,
        t.artists,
        JSON_VALUE(t.external_ids, '$.isrc') AS isrc,
        0 AS _source_priority,
        'top_tracks' AS _source_name,
        t._ingested_at
    FROM {{ ref('dlk_spotify_svc__top_tracks') }} AS t

    UNION ALL

    SELECT
        t.track_id,
        TO_JSON_STRING(t) AS track,
        t.album_id,
        CAST(NULL AS STRING) AS album,
        t.artists,
        CAST(NULL AS STRING) AS isrc,
        1 AS _source_priority,
        'album_tracks' AS _source_name,
        t._ingested_at
    FROM {{ ref('dlk_spotify_svc__album_tracks') }} AS t

    UNION ALL

    SELECT
        t.track_id,
        t.track,
        JSON_VALUE(t.track, '$.album.id') AS album_id,
        JSON_QUERY(t.track, '$.album') AS album,
        JSON_QUERY(t.track, '$.artists') AS artists,
        JSON_VALUE(t.track, '$.external_ids.isrc') AS isrc,
        2 AS _source_priority,
        'recently_played' AS _source_name,
        t._ingested_at
    FROM {{ ref('dlk_spotify_svc__recently_played') }} AS t

    UNION ALL

    SELECT
        t.track_id,
        t.track,
        JSON_VALUE(t.track, '$.album.id') AS album_id,
        JSON_QUERY(t.track, '$.album') AS album,
        JSON_QUERY(t.track, '$.artists') AS artists,
        JSON_VALUE(t.track, '$.external_ids.isrc') AS isrc,
        3 AS _source_priority,
        'saved_tracks' AS _source_name,
        t._ingested_at
    FROM {{ ref('dlk_spotify_svc__saved_tracks') }} AS t

    UNION ALL

    SELECT
        t.id AS track_id,
        TO_JSON_STRING(t) AS track,
        t.album.id AS album_id,
        TO_JSON_STRING(t.album) AS album,
        TO_JSON_STRING(t.artists) AS artists,
        t.external_ids.isrc,
        0 AS _source_priority,
        'legacy_top_tracks' AS _source_name,
        t._ingested_at
    FROM {{ ref('dlk_spotify_legacy_svc__top_tracks') }} AS t

    UNION ALL

    SELECT
        t.id AS track_id,
        TO_JSON_STRING(t) AS track,
        t.album_id,
        CAST(NULL AS STRING) AS album,
        TO_JSON_STRING(t.artists) AS artists,
        CAST(NULL AS STRING) AS isrc,
        1 AS _source_priority,
        'legacy_album_tracks' AS _source_name,
        t._ingested_at
    FROM {{ ref('dlk_spotify_legacy_svc__album_tracks') }} AS t

    UNION ALL

    SELECT
        t.track_id,
        TO_JSON_STRING(t.track) AS track,
        JSON_VALUE(TO_JSON_STRING(t.track), '$.album.id') AS album_id,
        JSON_QUERY(TO_JSON_STRING(t.track), '$.album') AS album,
        JSON_QUERY(TO_JSON_STRING(t.track), '$.artists') AS artists,
        JSON_VALUE(TO_JSON_STRING(t.track), '$.external_ids.isrc') AS isrc,
        2 AS _source_priority,
        'legacy_recently_played' AS _source_name,
        t._ingested_at
    FROM {{ ref('dlk_spotify_legacy_svc__recently_played') }} AS t

    UNION ALL

    SELECT
        t.track_id,
        TO_JSON_STRING(t.track) AS track,
        JSON_VALUE(TO_JSON_STRING(t.track), '$.album.id') AS album_id,
        JSON_QUERY(TO_JSON_STRING(t.track), '$.album') AS album,
        JSON_QUERY(TO_JSON_STRING(t.track), '$.artists') AS artists,
        JSON_VALUE(TO_JSON_STRING(t.track), '$.external_ids.isrc') AS isrc,
        3 AS _source_priority,
        'legacy_saved_tracks' AS _source_name,
        t._ingested_at
    FROM {{ ref('dlk_spotify_legacy_svc__saved_tracks') }} AS t
)

SELECT
    track_id,
    isrc,
    album,
    artists,
    _source_priority,
    _source_name,
    _ingested_at,
    NULLIF(TRIM(JSON_VALUE(track, '$.name')), '') AS track_name,
    JSON_VALUE(track, '$.uri') AS track_uri,
    SAFE_CAST(JSON_VALUE(track, '$.duration_ms') AS INT64) AS duration_ms,
    SAFE_CAST(JSON_VALUE(track, '$.explicit') AS BOOL) AS explicit,
    SAFE_CAST(JSON_VALUE(track, '$.track_number') AS INT64) AS track_number,
    SAFE_CAST(JSON_VALUE(track, '$.disc_number') AS INT64) AS disc_number,
    SAFE_CAST(JSON_VALUE(track, '$.popularity') AS INT64) AS popularity,
    NULLIF(TRIM(album_id), '') AS album_id,
    SAFE_CAST(JSON_VALUE(track, '$.is_local') AS BOOL) AS is_local
FROM observations
