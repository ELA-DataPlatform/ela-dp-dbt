{{
    config(
        materialized='incremental',
        enabled=false,
        unique_key='playlist_id',
        merge_update_columns=[
            'playlist_name', 'description', 'collaborative', 'public',
            'snapshot_id', 'owner_id', 'total_tracks', 'playlist_uri', '_ingested_at'
        ],
        tags=['spotify']
    )
}}

-- Reconcile every canonical staging row, including late records and corrections.
SELECT *
FROM {{ ref('hub_music_stg__ref_playlist') }}
