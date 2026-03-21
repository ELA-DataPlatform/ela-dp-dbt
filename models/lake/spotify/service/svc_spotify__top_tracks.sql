{%- set has_legacy = spotify_table_exists('normalized_top_tracks_legacy') -%}
{%- set has_new = spotify_table_exists('normalized_top_tracks') -%}

{{
    config(
        materialized='incremental',
        unique_key='id',
        merge_update_columns=[
            'type',
            'popularity',
            'uri',
            'name',
            'is_playable',
            'href',
            'external_urls',
            'preview_url',
            'duration_ms',
            'explicit',
            'track_number',
            'external_ids',
            'is_local',
            'disc_number',
            'available_markets',
            'artists',
            'album',
            '_ingested_at'
        ],
        tags=['spotify'],
        enabled=(has_legacy or has_new)
    )
}}

SELECT
    id,
    type,
    popularity,
    uri,
    name,
    is_playable,
    href,
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
    album,
    _ingested_at
FROM {{ ref('stg_spotify__top_tracks') }}

{% if is_incremental() %}
    WHERE _ingested_at > (SELECT max(_ingested_at) FROM {{ this }})
{% endif %}
