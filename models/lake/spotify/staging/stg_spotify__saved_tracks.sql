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
            JSON_VALUE(track, '$.type') AS type,
            CAST(JSON_VALUE(track, '$.popularity') AS INT64) AS popularity,
            JSON_VALUE(track, '$.uri') AS uri,
            JSON_VALUE(track, '$.name') AS name,
            CAST(JSON_VALUE(track, '$.is_playable') AS BOOL) AS is_playable,
            JSON_VALUE(track, '$.href') AS href,
            JSON_VALUE(track, '$.id') AS id,
            STRUCT(JSON_VALUE(track, '$.external_urls.spotify') AS spotify) AS external_urls,
            JSON_VALUE(track, '$.preview_url') AS preview_url,
            CAST(JSON_VALUE(track, '$.duration_ms') AS INT64) AS duration_ms,
            CAST(JSON_VALUE(track, '$.explicit') AS BOOL) AS explicit,
            CAST(JSON_VALUE(track, '$.track_number') AS INT64) AS track_number,
            STRUCT(JSON_VALUE(track, '$.external_ids.isrc') AS isrc) AS external_ids,
            CAST(JSON_VALUE(track, '$.is_local') AS BOOL) AS is_local,
            CAST(JSON_VALUE(track, '$.disc_number') AS INT64) AS disc_number,
            JSON_VALUE_ARRAY(track, '$.available_markets') AS available_markets,
            ARRAY(
                SELECT STRUCT(
                    JSON_VALUE(artist, '$.name') AS name,
                    STRUCT(JSON_VALUE(artist, '$.external_urls.spotify') AS spotify) AS external_urls,
                    JSON_VALUE(artist, '$.id') AS id,
                    JSON_VALUE(artist, '$.uri') AS uri,
                    JSON_VALUE(artist, '$.href') AS href,
                    JSON_VALUE(artist, '$.type') AS type
                )
                FROM UNNEST(JSON_QUERY_ARRAY(track, '$.artists')) AS artist
            ) AS artists,
            STRUCT(
                JSON_VALUE(track, '$.album.type') AS type,
                CAST(JSON_VALUE(track, '$.album.total_tracks') AS INT64) AS total_tracks,
                JSON_VALUE(track, '$.album.release_date') AS release_date,
                JSON_VALUE(track, '$.album.uri') AS uri,
                JSON_VALUE(track, '$.album.name') AS name,
                CAST(JSON_VALUE(track, '$.album.is_playable') AS BOOL) AS is_playable,
                JSON_VALUE(track, '$.album.href') AS href,
                JSON_VALUE(track, '$.album.id') AS id,
                STRUCT(JSON_VALUE(track, '$.album.external_urls.spotify') AS spotify) AS external_urls,
                ARRAY(
                    SELECT STRUCT(
                        JSON_VALUE(a, '$.name') AS name,
                        STRUCT(JSON_VALUE(a, '$.external_urls.spotify') AS spotify) AS external_urls,
                        JSON_VALUE(a, '$.id') AS id,
                        JSON_VALUE(a, '$.uri') AS uri,
                        JSON_VALUE(a, '$.href') AS href,
                        JSON_VALUE(a, '$.type') AS type
                    )
                    FROM UNNEST(JSON_QUERY_ARRAY(track, '$.album.artists')) AS a
                ) AS artists,
                JSON_VALUE(track, '$.album.release_date_precision') AS release_date_precision,
                ARRAY(
                    SELECT STRUCT(
                        JSON_VALUE(img, '$.url') AS url,
                        CAST(JSON_VALUE(img, '$.width') AS INT64) AS width,
                        CAST(JSON_VALUE(img, '$.height') AS INT64) AS height
                    )
                    FROM UNNEST(JSON_QUERY_ARRAY(track, '$.album.images')) AS img
                ) AS images,
                JSON_VALUE_ARRAY(track, '$.album.available_markets') AS available_markets,
                JSON_VALUE(track, '$.album.album_type') AS album_type
            ) AS album
        ) AS track,
        _ingested_at
    FROM {{ source('spotify', 'normalized_saved_tracks') }}
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY track.id
            ORDER BY _ingested_at DESC
        ) AS _row_number
    FROM source
)

SELECT * EXCEPT(_row_number)
FROM deduplicated
WHERE _row_number = 1
