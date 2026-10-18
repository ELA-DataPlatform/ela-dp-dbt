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

-- Reconcile every canonical staging row, including late records and corrections.
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
FROM {{ ref('dlk_spotify_legacy_stg__album_tracks') }}
