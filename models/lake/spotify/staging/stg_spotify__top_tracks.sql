{%- set has_legacy = spotify_table_exists('normalized_top_tracks_legacy') -%}
{%- set has_new = spotify_table_exists('normalized_top_tracks') -%}

{{
    config(
        materialized='view',
        tags=['spotify'],
        enabled=(has_legacy or has_new)
    )
}}

WITH

{% if has_legacy %}
from_legacy AS (
    SELECT
        _ingested_at,
        type,
        popularity,
        uri,
        name,
        is_playable,
        href,
        id,
        external_urls,
        preview_url,
        duration_ms,
        explicit,
        track_number,
        external_ids,
        is_local,
        disc_number,
        available_markets,
        artists,
        album
    FROM {{ source('spotify', 'normalized_top_tracks_legacy') }}
),
{% endif %}

{% if has_new %}
from_new AS (
    SELECT
        _ingested_at,
        type,
        popularity,
        uri,
        name,
        is_playable,
        href,
        id,
        STRUCT(
            JSON_VALUE(external_urls, '$.spotify') AS spotify
        ) AS external_urls,
        preview_url,
        duration_ms,
        explicit,
        track_number,
        STRUCT(
            JSON_VALUE(external_ids, '$.isrc') AS isrc
        ) AS external_ids,
        is_local,
        disc_number,
        JSON_VALUE_ARRAY(available_markets, '$') AS available_markets,
        ARRAY(
            SELECT STRUCT(
                JSON_VALUE(artist, '$.name') AS name,
                STRUCT(JSON_VALUE(artist, '$.external_urls.spotify') AS spotify) AS external_urls,
                JSON_VALUE(artist, '$.id') AS id,
                JSON_VALUE(artist, '$.uri') AS uri,
                JSON_VALUE(artist, '$.href') AS href,
                JSON_VALUE(artist, '$.type') AS type
            )
            FROM UNNEST(JSON_QUERY_ARRAY(artists, '$')) AS artist
        ) AS artists,
        STRUCT(
            JSON_VALUE(album, '$.type') AS type,
            CAST(JSON_VALUE(album, '$.total_tracks') AS INT64) AS total_tracks,
            JSON_VALUE(album, '$.release_date') AS release_date,
            JSON_VALUE(album, '$.uri') AS uri,
            JSON_VALUE(album, '$.name') AS name,
            CAST(JSON_VALUE(album, '$.is_playable') AS BOOL) AS is_playable,
            JSON_VALUE(album, '$.href') AS href,
            JSON_VALUE(album, '$.id') AS id,
            STRUCT(JSON_VALUE(album, '$.external_urls.spotify') AS spotify) AS external_urls,
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
            JSON_VALUE(album, '$.release_date_precision') AS release_date_precision,
            ARRAY(
                SELECT STRUCT(
                    CAST(JSON_VALUE(img, '$.width') AS INT64) AS width,
                    JSON_VALUE(img, '$.url') AS url,
                    CAST(JSON_VALUE(img, '$.height') AS INT64) AS height
                )
                FROM UNNEST(JSON_QUERY_ARRAY(album, '$.images')) AS img
            ) AS images,
            JSON_VALUE_ARRAY(album, '$.available_markets') AS available_markets,
            JSON_VALUE(album, '$.album_type') AS album_type
        ) AS album
    FROM {{ source('spotify', 'normalized_top_tracks') }}
),
{% endif %}

unioned AS (
    {% if has_legacy and has_new %}
    SELECT * FROM from_legacy
    UNION ALL
    SELECT * FROM from_new
    {% elif has_legacy %}
    SELECT * FROM from_legacy
    {% elif has_new %}
    SELECT * FROM from_new
    {% endif %}
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY _ingested_at DESC
        ) AS _row_number
    FROM unioned
)

SELECT * EXCEPT(_row_number)
FROM deduplicated
WHERE _row_number = 1
