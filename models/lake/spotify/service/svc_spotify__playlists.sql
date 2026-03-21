{%- set has_legacy = spotify_table_exists('normalized_playlists_legacy') -%}
{%- set has_new = spotify_table_exists('normalized_playlists') -%}

{{
    config(
        materialized='incremental',
        unique_key='id',
        merge_update_columns=[
            'collaborative',
            'snapshot_id',
            'public',
            'images',
            'owner',
            'type',
            'primary_color',
            'external_urls',
            'tracks',
            'href',
            'uri',
            'items',
            'name',
            'description',
            '_ingested_at'
        ],
        tags=['spotify'],
        enabled=(has_legacy or has_new)
    )
}}

SELECT
    id,
    collaborative,
    snapshot_id,
    public,
    images,
    owner,
    type,
    primary_color,
    external_urls,
    tracks,
    href,
    uri,
    items,
    name,
    description,
    _ingested_at
FROM {{ ref('stg_spotify__playlists') }}

{% if is_incremental() %}
    WHERE _ingested_at > (SELECT max(_ingested_at) FROM {{ this }})
{% endif %}
