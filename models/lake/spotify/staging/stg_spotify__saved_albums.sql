{{
    config(
        materialized='view',
        tags=['spotify']
    )
}}

WITH source AS (
    SELECT
        added_at,
        STRUCT(
            STRUCT(CAST(JSON_VALUE(album, '$.external_ids.upc') AS INT64) AS upc) AS external_ids,
            ARRAY(
                SELECT STRUCT(
                    JSON_VALUE(c, '$.type') AS type,
                    JSON_VALUE(c, '$.text') AS text
                )
                FROM UNNEST(JSON_QUERY_ARRAY(album, '$.copyrights')) AS c
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
                FROM UNNEST(JSON_QUERY_ARRAY(album, '$.artists')) AS a
            ) AS artists,
            JSON_VALUE(album, '$.release_date') AS release_date,
            JSON_VALUE(album, '$.label') AS label,
            JSON_VALUE(album, '$.uri') AS uri,
            JSON_VALUE(album, '$.name') AS name,
            CAST(JSON_VALUE(album, '$.popularity') AS INT64) AS popularity,
            JSON_VALUE(album, '$.type') AS type,
            JSON_VALUE(album, '$.href') AS href,
            JSON_VALUE_ARRAY(album, '$.genres') AS genres,
            STRUCT(
                CAST(JSON_VALUE(album, '$.tracks.total') AS INT64) AS total,
                JSON_VALUE(album, '$.tracks.previous') AS previous,
                JSON_VALUE(album, '$.tracks.next') AS next,
                CAST(JSON_VALUE(album, '$.tracks.limit') AS INT64) AS `limit`,
                CAST(JSON_VALUE(album, '$.tracks.offset') AS INT64) AS offset,
                ARRAY(
                    SELECT STRUCT(
                        STRUCT(JSON_VALUE(item, '$.restrictions.reason') AS reason) AS restrictions,
                        JSON_VALUE(item, '$.type') AS type,
                        CAST(JSON_VALUE(item, '$.duration_ms') AS INT64) AS duration_ms,
                        CAST(JSON_VALUE(item, '$.explicit') AS BOOL) AS explicit,
                        JSON_VALUE(item, '$.preview_url') AS preview_url,
                        JSON_VALUE(item, '$.uri') AS uri,
                        JSON_VALUE(item, '$.name') AS name,
                        JSON_VALUE(item, '$.href') AS href,
                        JSON_VALUE(item, '$.id') AS id,
                        STRUCT(JSON_VALUE(item, '$.external_urls.spotify') AS spotify) AS external_urls,
                        STRUCT(
                            JSON_VALUE(item, '$.linked_from.uri') AS uri,
                            JSON_VALUE(item, '$.linked_from.id') AS id,
                            JSON_VALUE(item, '$.linked_from.href') AS href,
                            JSON_VALUE(item, '$.linked_from.type') AS type,
                            STRUCT(JSON_VALUE(item, '$.linked_from.external_urls.spotify') AS spotify) AS external_urls
                        ) AS linked_from,
                        CAST(JSON_VALUE(item, '$.is_local') AS BOOL) AS is_local,
                        CAST(JSON_VALUE(item, '$.disc_number') AS INT64) AS disc_number,
                        JSON_VALUE_ARRAY(item, '$.available_markets') AS available_markets,
                        CAST(JSON_VALUE(item, '$.track_number') AS INT64) AS track_number,
                        ARRAY(
                            SELECT STRUCT(
                                JSON_VALUE(ta, '$.name') AS name,
                                STRUCT(JSON_VALUE(ta, '$.external_urls.spotify') AS spotify) AS external_urls,
                                JSON_VALUE(ta, '$.id') AS id,
                                JSON_VALUE(ta, '$.uri') AS uri,
                                JSON_VALUE(ta, '$.href') AS href,
                                JSON_VALUE(ta, '$.type') AS type
                            )
                            FROM UNNEST(JSON_QUERY_ARRAY(item, '$.artists')) AS ta
                        ) AS artists
                    )
                    FROM UNNEST(JSON_QUERY_ARRAY(album, '$.tracks.items')) AS item
                ) AS items,
                JSON_VALUE(album, '$.tracks.href') AS href
            ) AS tracks,
            JSON_VALUE(album, '$.id') AS id,
            STRUCT(JSON_VALUE(album, '$.external_urls.spotify') AS spotify) AS external_urls,
            CAST(JSON_VALUE(album, '$.total_tracks') AS INT64) AS total_tracks,
            JSON_VALUE(album, '$.release_date_precision') AS release_date_precision,
            ARRAY(
                SELECT STRUCT(
                    CAST(JSON_VALUE(img, '$.width') AS INT64) AS width,
                    CAST(JSON_VALUE(img, '$.height') AS INT64) AS height,
                    JSON_VALUE(img, '$.url') AS url
                )
                FROM UNNEST(JSON_QUERY_ARRAY(album, '$.images')) AS img
            ) AS images,
            JSON_VALUE_ARRAY(album, '$.available_markets') AS available_markets,
            JSON_VALUE(album, '$.album_type') AS album_type
        ) AS album,
        _ingested_at
    FROM {{ source('spotify', 'normalized_saved_albums') }}
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY album.id
            ORDER BY _ingested_at DESC
        ) AS _row_number
    FROM source
)

SELECT * EXCEPT(_row_number)
FROM deduplicated
WHERE _row_number = 1
