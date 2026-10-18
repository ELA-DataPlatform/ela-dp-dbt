{{
    config(
        materialized='incremental',
        unique_key=['played_at', 'track_id'],
        merge_update_columns=['album_id', 'context_type', 'context_uri', '_ingested_at'],
        tags=['spotify'],
        partition_by={
            'field': 'played_at',
            'data_type': 'timestamp',
            'granularity': 'month'
        }
    )
}}

-- Merge the complete deduplicated source: late and equal-timestamp corrections are replayed.
SELECT *
FROM {{ ref('hub_music_stg__fact_played') }}
