{%- set has_legacy = spotify_table_exists('normalized_top_artists_legacy') -%}
{%- set has_new = spotify_table_exists('normalized_top_artists') -%}

{{
    config(
        materialized='incremental',
        unique_key='id',
        merge_update_columns=[
            'popularity',
            'name',
            'images',
            'uri',
            'type',
            'href',
            'genres',
            'followers',
            'external_urls',
            '_ingested_at'
        ],
        tags=['spotify'],
        enabled=(has_legacy or has_new)
    )
}}

SELECT
    id,
    popularity,
    name,
    images,
    uri,
    type,
    href,
    genres,
    followers,
    external_urls,
    _ingested_at
FROM {{ ref('stg_spotify__top_artists') }}

{% if is_incremental() %}
    WHERE _ingested_at > (SELECT max(_ingested_at) FROM {{ this }})
{% endif %}
