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
        tags=['spotify']
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
FROM {{ ref('stg_spotify_legacy__album_tracks') }}

{% if is_incremental() %}
    WHERE _ingested_at > (SELECT MAX(_ingested_at) FROM {{ this }})
{% endif %}
