{{
    config(
        materialized='view',
        tags=['spotify']
    )
}}

WITH source AS (
    SELECT
        _ingested_at,
        album_id,
        id,
        type,
        duration_ms,
        explicit,
        preview_url,
        uri,
        name,
        href,
        STRUCT(JSON_VALUE(external_urls, '$.spotify') AS spotify) AS external_urls,
        is_local,
        disc_number,
        JSON_VALUE_ARRAY(available_markets, '$') AS available_markets,
        track_number,
        ARRAY(
            SELECT STRUCT(
                JSON_VALUE(a, '$.name') AS name,
                STRUCT(JSON_VALUE(a, '$.external_urls.spotify') AS spotify) AS external_urls,
                JSON_VALUE(a, '$.id') AS id,
                JSON_VALUE(a, '$.uri') AS uri,
                JSON_VALUE(a, '$.href') AS href,
                JSON_VALUE(a, '$.type') AS type
            )
            FROM UNNEST(JSON_QUERY_ARRAY(artists, '$')) AS a
        ) AS artists
    FROM {{ source('spotify', 'normalized_album_tracks') }}
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY album_id, id
            ORDER BY _ingested_at DESC
        ) AS _row_number
    FROM source
)

SELECT * EXCEPT(_row_number)
FROM deduplicated
WHERE _row_number = 1
