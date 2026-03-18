{{
    config(
        materialized='incremental',
        unique_key='track_id',
        merge_update_columns=[
            'name',
            'artists',
            'album',
            'duration_ms',
            'explicit',
            'preview_url',
            'uri',
            'href',
            'external_urls',
            'external_ids',
            'is_local',
            'disc_number',
            'available_markets',
            'track_number',
            'type',
            'popularity',
            'is_playable',
            '_ingested_at'
        ],
        tags=['spotify']
    )
}}

SELECT
    id AS track_id,
    name,
    artists,
    album,
    duration_ms,
    explicit,
    preview_url,
    uri,
    href,
    external_urls,
    external_ids,
    is_local,
    disc_number,
    available_markets,
    track_number,
    type,
    popularity,
    is_playable,
    _ingested_at
FROM {{ ref('stg_spotify__top_tracks') }}

{% if is_incremental() %}
    WHERE _ingested_at > (SELECT max(_ingested_at) FROM {{ this }})
{% endif %}
