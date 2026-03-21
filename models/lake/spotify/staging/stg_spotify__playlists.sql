{%- set has_legacy = spotify_table_exists('normalized_playlists_legacy') -%}
{%- set has_new = spotify_table_exists('normalized_playlists') -%}

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
        collaborative,
        snapshot_id,
        public,
        images,
        owner,
        type,
        primary_color,
        external_urls,
        tracks,
        id,
        href,
        uri,
        items,
        name,
        description
    FROM {{ source('spotify', 'normalized_playlists_legacy') }}
),
{% endif %}

{% if has_new %}
from_new AS (
    SELECT
        _ingested_at,
        collaborative,
        snapshot_id,
        public,
        ARRAY(
            SELECT STRUCT(
                CAST(JSON_VALUE(img, '$.width') AS INT64) AS width,
                JSON_VALUE(img, '$.url') AS url,
                CAST(JSON_VALUE(img, '$.height') AS INT64) AS height
            )
            FROM UNNEST(JSON_QUERY_ARRAY(images, '$')) AS img
        ) AS images,
        STRUCT(
            STRUCT(JSON_VALUE(owner, '$.external_urls.spotify') AS spotify) AS external_urls,
            JSON_VALUE(owner, '$.id') AS id,
            JSON_VALUE(owner, '$.uri') AS uri,
            JSON_VALUE(owner, '$.href') AS href,
            JSON_VALUE(owner, '$.type') AS type,
            JSON_VALUE(owner, '$.display_name') AS display_name
        ) AS owner,
        type,
        primary_color,
        STRUCT(
            JSON_VALUE(external_urls, '$.spotify') AS spotify
        ) AS external_urls,
        STRUCT(
            CAST(JSON_VALUE(tracks, '$.total') AS INT64) AS total,
            JSON_VALUE(tracks, '$.href') AS href
        ) AS tracks,
        id,
        href,
        uri,
        STRUCT(
            CAST(JSON_VALUE(items, '$.total') AS INT64) AS total,
            JSON_VALUE(items, '$.href') AS href
        ) AS items,
        name,
        description
    FROM {{ source('spotify', 'normalized_playlists') }}
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
