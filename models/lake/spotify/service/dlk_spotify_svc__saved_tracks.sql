{{
    config(
        materialized='incremental',
        unique_key='track_id',
        merge_update_columns=[
            'track',
            'added_at',
            '_ingested_at'
        ],
        tags=['spotify']
    )
}}

-- Reconcile every canonical staging row, including late records and corrections.
SELECT
    track,
    added_at,
    _ingested_at,
    JSON_VALUE(track, '$.id') AS track_id
FROM {{ ref('dlk_spotify_stg__saved_tracks') }}
