{{ config(materialized='view', tags=['spotify']) }}

-- Includes minimal album references from album_tracks, even without a payload.
WITH observations AS (
    SELECT
        t.album_id,
        TO_JSON_STRING(t) AS album,
        t.artists,
        t.genres,
        t.images,
        0 AS _source_priority,
        'album_detail' AS _source_name,
        TRUE AS _is_detail,
        t._ingested_at
    FROM {{ ref('dlk_spotify_svc__album_detail') }} AS t

    UNION ALL

    SELECT
        t.album_id,
        t.album,
        JSON_QUERY(t.album, '$.artists') AS artists,
        JSON_QUERY(t.album, '$.genres') AS genres,
        JSON_QUERY(t.album, '$.images') AS images,
        1 AS _source_priority,
        'saved_albums' AS _source_name,
        FALSE AS _is_detail,
        t._ingested_at
    FROM {{ ref('dlk_spotify_svc__saved_albums') }} AS t

    UNION ALL

    SELECT
        t.id AS album_id,
        TO_JSON_STRING(t) AS album,
        TO_JSON_STRING(t.artists) AS artists,
        TO_JSON_STRING(t.genres) AS genres,
        TO_JSON_STRING(t.images) AS images,
        0 AS _source_priority,
        'legacy_album_detail' AS _source_name,
        TRUE AS _is_detail,
        t._ingested_at
    FROM {{ ref('dlk_spotify_legacy_svc__album_detail') }} AS t

    UNION ALL

    SELECT
        t.album_id,
        TO_JSON_STRING(t.album) AS album,
        JSON_QUERY(TO_JSON_STRING(t.album), '$.artists') AS artists,
        JSON_QUERY(TO_JSON_STRING(t.album), '$.genres') AS genres,
        JSON_QUERY(TO_JSON_STRING(t.album), '$.images') AS images,
        1 AS _source_priority,
        'legacy_saved_albums' AS _source_name,
        FALSE AS _is_detail,
        t._ingested_at
    FROM {{ ref('dlk_spotify_legacy_svc__saved_albums') }} AS t

    UNION ALL

    SELECT
        album_id,
        album,
        JSON_QUERY(album, '$.artists') AS artists,
        JSON_QUERY(album, '$.genres') AS genres,
        JSON_QUERY(album, '$.images') AS images,
        2 AS _source_priority,
        _source_name,
        FALSE AS _is_detail,
        _ingested_at
    FROM {{ ref('hub_music_stg__track_observations') }}
)

SELECT
    album_id,
    genres,
    artists,
    _source_priority,
    _source_name,
    _is_detail,
    _ingested_at,
    NULLIF(TRIM(JSON_VALUE(album, '$.name')), '') AS album_name,
    JSON_VALUE(album, '$.album_type') AS album_type,
    SAFE_CAST(JSON_VALUE(album, '$.total_tracks') AS INT64) AS total_tracks,
    JSON_VALUE(album, '$.release_date') AS release_date,
    JSON_VALUE(album, '$.release_date_precision') AS release_date_precision,
    JSON_VALUE(album, '$.uri') AS album_uri,
    JSON_VALUE(album, '$.label') AS label,
    SAFE_CAST(JSON_VALUE(album, '$.popularity') AS INT64) AS popularity,
    JSON_VALUE(images, '$[0].url') AS album_image_url
FROM observations
WHERE NULLIF(TRIM(album_id), '') IS NOT NULL
