{{
    config(
        materialized='incremental',
        unique_key=['album_id', 'track_id'],
        merge_update_columns=[
            'name',
            'artists',
            'duration_ms',
            'explicit',
            'preview_url',
            'uri',
            'href',
            'external_urls',
            'is_local',
            'disc_number',
            'available_markets',
            'track_number',
            'type',
            'restrictions',
            '_ingested_at'
        ],
        tags=['spotify']
    )
}}

SELECT
    album_id,
    id AS track_id,
    name,
    artists,
    duration_ms,
    explicit,
    preview_url,
    uri,
    href,
    external_urls,
    is_local,
    disc_number,
    available_markets,
    track_number,
    type,
    restrictions,
    _ingested_at
FROM {{ ref('stg_spotify__album_tracks') }}

{% if is_incremental() %}
    WHERE _ingested_at > (SELECT max(_ingested_at) FROM {{ this }})
{% endif %}
