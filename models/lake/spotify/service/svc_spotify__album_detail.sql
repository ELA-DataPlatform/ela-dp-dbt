{%- set has_legacy = spotify_table_exists('normalized_album_detail_legacy') -%}

{{
    config(
        materialized='incremental',
        unique_key='id',
        merge_update_columns=[
            'external_ids',
            'copyrights',
            'artists',
            'release_date',
            'label',
            'uri',
            'name',
            'popularity',
            'type',
            'href',
            'genres',
            'tracks',
            'external_urls',
            'total_tracks',
            'release_date_precision',
            'images',
            'available_markets',
            'album_type',
            '_ingested_at'
        ],
        tags=['spotify'],
        enabled=has_legacy
    )
}}

SELECT
    id,
    external_ids,
    copyrights,
    artists,
    release_date,
    label,
    uri,
    name,
    popularity,
    type,
    href,
    genres,
    tracks,
    external_urls,
    total_tracks,
    release_date_precision,
    images,
    available_markets,
    album_type,
    _ingested_at
FROM {{ ref('stg_spotify__album_detail') }}

{% if is_incremental() %}
    WHERE _ingested_at > (SELECT max(_ingested_at) FROM {{ this }})
{% endif %}
