{{
    config(
        materialized='incremental',
        unique_key=['played_at', 'track_id'],
        merge_update_columns=[
            'track',
            'context',
            '_ingested_at'
        ],
        tags=['spotify'],
        partition_by={
            'field': 'played_at',
            'data_type': 'timestamp',
            'granularity': 'month'
        }
    )
}}

-- Reconcile every canonical staging row, including late records and corrections.
SELECT
    played_at,
    track,
    context,
    _ingested_at,
    JSON_VALUE(track, '$.id') AS track_id
FROM {{ ref('dlk_spotify_stg__recently_played') }}
