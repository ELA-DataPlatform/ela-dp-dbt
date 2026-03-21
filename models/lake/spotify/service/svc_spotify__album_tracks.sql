{%- set has_legacy = spotify_table_exists('normalized_album_tracks_legacy') -%}

{{
    config(
        materialized='incremental',
        unique_key=['album_id', 'id'],
        merge_update_columns=[
            'restrictions',
            'type',
            'duration_ms',
            'explicit',
            'preview_url',
            'uri',
            'name',
            'href',
            'external_urls',
            'is_local',
            'disc_number',
            'available_markets',
            'track_number',
            'artists',
            '_ingested_at'
        ],
        tags=['spotify'],
        enabled=has_legacy
    )
}}

SELECT
    album_id,
    id,
    restrictions,
    type,
    duration_ms,
    explicit,
    preview_url,
    uri,
    name,
    href,
    external_urls,
    is_local,
    disc_number,
    available_markets,
    track_number,
    artists,
    _ingested_at
FROM {{ ref('stg_spotify__album_tracks') }}

{% if is_incremental() %}
    WHERE _ingested_at > (SELECT max(_ingested_at) FROM {{ this }})
{% endif %}
