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
        tags=['spotify']
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
FROM {{ ref('dlk_spotify_legacy_stg__top_tracks') }}

{% if is_incremental() %}
    WHERE _ingested_at > (SELECT MAX(_ingested_at) FROM {{ this }})
{% endif %}
