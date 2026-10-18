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
            '_ingested_at'
        ],
        tags=['spotify']
    )
}}

-- Reconcile every canonical staging row, including late records and corrections.
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
    _ingested_at
FROM {{ ref('dlk_spotify_stg__album_tracks') }}
