{{
    config(
        materialized='incremental',
        unique_key='playlist_id',
        merge_update_columns=[
            'name',
            'description',
            'collaborative',
            'public',
            'snapshot_id',
            'images',
            'owner',
            'type',
            'primary_color',
            'external_urls',
            'tracks',
            'items',
            'href',
            'uri',
            '_ingested_at'
        ],
        tags=['spotify']
    )
}}

SELECT
    id AS playlist_id,
    name,
    description,
    collaborative,
    public,
    snapshot_id,
    images,
    owner,
    type,
    primary_color,
    external_urls,
    tracks,
    items,
    href,
    uri,
    _ingested_at
FROM {{ ref('stg_spotify__playlists') }}

{% if is_incremental() %}
    WHERE _ingested_at > (SELECT max(_ingested_at) FROM {{ this }})
{% endif %}
