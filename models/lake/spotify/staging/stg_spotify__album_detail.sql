{{
    config(
        materialized='view',
        tags=['spotify']
    )
}}

WITH source AS (
    SELECT
        _ingested_at,
        id,
        STRUCT(JSON_VALUE(external_ids, '$.upc') AS upc) AS external_ids,
        ARRAY(
            SELECT STRUCT(
                JSON_VALUE(c, '$.type') AS type,
                JSON_VALUE(c, '$.text') AS text
            )
            FROM UNNEST(JSON_QUERY_ARRAY(copyrights, '$')) AS c
        ) AS copyrights,
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
        ) AS artists,
        release_date,
        label,
        uri,
        name,
        popularity,
        type,
        href,
        JSON_VALUE_ARRAY(genres, '$') AS genres,
        tracks,
        STRUCT(JSON_VALUE(external_urls, '$.spotify') AS spotify) AS external_urls,
        total_tracks,
        release_date_precision,
        ARRAY(
            SELECT STRUCT(
                CAST(JSON_VALUE(img, '$.width') AS INT64) AS width,
                CAST(JSON_VALUE(img, '$.height') AS INT64) AS height,
                JSON_VALUE(img, '$.url') AS url
            )
            FROM UNNEST(JSON_QUERY_ARRAY(images, '$')) AS img
        ) AS images,
        JSON_VALUE_ARRAY(available_markets, '$') AS available_markets,
        album_type
    FROM {{ source('spotify', 'normalized_album_detail') }}
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY _ingested_at DESC
        ) AS _row_number
    FROM source
)

SELECT * EXCEPT(_row_number)
FROM deduplicated
WHERE _row_number = 1
