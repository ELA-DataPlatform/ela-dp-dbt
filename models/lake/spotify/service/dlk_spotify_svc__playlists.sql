{{
    config(
        materialized='incremental',
        enabled=false,
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

-- Reconcile every canonical staging row, including late records and corrections.
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
FROM {{ ref('dlk_spotify_stg__playlists') }}
