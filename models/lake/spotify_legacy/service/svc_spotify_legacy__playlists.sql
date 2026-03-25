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
        tags=['spotify']
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
FROM {{ ref('stg_spotify_legacy__playlists') }}

{% if is_incremental() %}
    WHERE _ingested_at > (SELECT max(_ingested_at) FROM {{ this }})
{% endif %}
