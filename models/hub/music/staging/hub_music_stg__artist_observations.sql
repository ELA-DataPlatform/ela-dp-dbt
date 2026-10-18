{{ config(materialized='view', tags=['spotify']) }}

-- All artists observed anywhere in the catalogue, including unplayed album tracks.
WITH observations AS (
    SELECT
        t.artist_id,
        NULLIF(TRIM(t.name), '') AS artist_name,
        t.uri AS artist_uri,
        t.genres,
        SAFE_CAST(JSON_VALUE(t.followers, '$.total') AS INT64) AS followers_total,
        t.popularity,
        JSON_VALUE(t.images, '$[0].url') AS artist_image_url,
        0 AS _source_priority,
        'artist_detail' AS _source_name,
        TRUE AS _is_detail,
        t._ingested_at
    FROM {{ ref('dlk_spotify_svc__artist_detail') }} AS t

    UNION ALL

    SELECT
        t.artist_id,
        NULLIF(TRIM(t.name), '') AS artist_name,
        t.uri AS artist_uri,
        t.genres,
        SAFE_CAST(JSON_VALUE(t.followers, '$.total') AS INT64) AS followers_total,
        t.popularity,
        JSON_VALUE(t.images, '$[0].url') AS artist_image_url,
        1 AS _source_priority,
        'top_artists' AS _source_name,
        FALSE AS _is_detail,
        t._ingested_at
    FROM {{ ref('dlk_spotify_svc__top_artists') }} AS t

    UNION ALL

    SELECT
        t.id AS artist_id,
        NULLIF(TRIM(t.name), '') AS artist_name,
        t.uri AS artist_uri,
        TO_JSON_STRING(t.genres) AS genres,
        t.followers.total AS followers_total,
        t.popularity,
        JSON_VALUE(TO_JSON_STRING(t.images), '$[0].url') AS artist_image_url,
        0 AS _source_priority,
        'legacy_artist_detail' AS _source_name,
        TRUE AS _is_detail,
        t._ingested_at
    FROM {{ ref('dlk_spotify_legacy_svc__artist_detail') }} AS t

    UNION ALL

    SELECT
        t.id AS artist_id,
        NULLIF(TRIM(t.name), '') AS artist_name,
        t.uri AS artist_uri,
        TO_JSON_STRING(t.genres) AS genres,
        t.followers.total AS followers_total,
        t.popularity,
        JSON_VALUE(TO_JSON_STRING(t.images), '$[0].url') AS artist_image_url,
        1 AS _source_priority,
        'legacy_top_artists' AS _source_name,
        FALSE AS _is_detail,
        t._ingested_at
    FROM {{ ref('dlk_spotify_legacy_svc__top_artists') }} AS t

    UNION ALL

    SELECT
        JSON_VALUE(artist, '$.id') AS artist_id,
        NULLIF(TRIM(JSON_VALUE(artist, '$.name')), '') AS artist_name,
        JSON_VALUE(artist, '$.uri') AS artist_uri,
        CAST(NULL AS STRING) AS genres,
        CAST(NULL AS INT64) AS followers_total,
        CAST(NULL AS INT64) AS popularity,
        CAST(NULL AS STRING) AS artist_image_url,
        2 AS _source_priority,
        CONCAT('track:', o._source_name) AS _source_name,
        FALSE AS _is_detail,
        o._ingested_at
    FROM {{ ref('hub_music_stg__track_observations') }} AS o,
        UNNEST(JSON_QUERY_ARRAY(o.artists)) AS artist

    UNION ALL

    SELECT
        JSON_VALUE(artist, '$.id') AS artist_id,
        NULLIF(TRIM(JSON_VALUE(artist, '$.name')), '') AS artist_name,
        JSON_VALUE(artist, '$.uri') AS artist_uri,
        CAST(NULL AS STRING) AS genres,
        CAST(NULL AS INT64) AS followers_total,
        CAST(NULL AS INT64) AS popularity,
        CAST(NULL AS STRING) AS artist_image_url,
        2 AS _source_priority,
        CONCAT('album:', o._source_name) AS _source_name,
        FALSE AS _is_detail,
        o._ingested_at
    FROM {{ ref('hub_music_stg__album_observations') }} AS o,
        UNNEST(JSON_QUERY_ARRAY(o.artists)) AS artist
)

SELECT *
FROM observations
WHERE NULLIF(TRIM(artist_id), '') IS NOT NULL
