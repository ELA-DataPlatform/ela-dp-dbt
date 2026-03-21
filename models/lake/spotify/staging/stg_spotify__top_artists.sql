{%- set has_legacy = spotify_table_exists('normalized_top_artists_legacy') -%}
{%- set has_new = spotify_table_exists('normalized_top_artists') -%}

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
        popularity,
        name,
        images,
        uri,
        type,
        href,
        genres,
        id,
        followers,
        external_urls
    FROM {{ source('spotify', 'normalized_top_artists_legacy') }}
),
{% endif %}

{% if has_new %}
from_new AS (
    SELECT
        _ingested_at,
        popularity,
        name,
        ARRAY(
            SELECT STRUCT(
                CAST(JSON_VALUE(img, '$.width') AS INT64) AS width,
                JSON_VALUE(img, '$.url') AS url,
                CAST(JSON_VALUE(img, '$.height') AS INT64) AS height
            )
            FROM UNNEST(JSON_QUERY_ARRAY(images, '$')) AS img
        ) AS images,
        uri,
        type,
        href,
        JSON_VALUE_ARRAY(genres, '$') AS genres,
        id,
        STRUCT(
            CAST(JSON_VALUE(followers, '$.total') AS INT64) AS total,
            JSON_VALUE(followers, '$.href') AS href
        ) AS followers,
        STRUCT(
            JSON_VALUE(external_urls, '$.spotify') AS spotify
        ) AS external_urls
    FROM {{ source('spotify', 'normalized_top_artists') }}
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
