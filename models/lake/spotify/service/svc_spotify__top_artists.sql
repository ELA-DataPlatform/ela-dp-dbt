{{
    config(
        materialized='incremental',
        unique_key='artist_id',
        merge_update_columns=[
            'name',
            'images',
            'uri',
            'type',
            'href',
            'genres',
            'followers',
            'external_urls',
            'popularity',
            '_ingested_at'
        ],
        tags=['spotify']
    )
}}

SELECT
    id AS artist_id,
    name,
    images,
    uri,
    type,
    href,
    genres,
    followers,
    external_urls,
    popularity,
    _ingested_at
FROM {{ ref('stg_spotify__top_artists') }}

{% if is_incremental() %}
    WHERE _ingested_at > (SELECT max(_ingested_at) FROM {{ this }})
{% endif %}
