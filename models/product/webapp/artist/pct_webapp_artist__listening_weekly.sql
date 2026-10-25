{{
    config(
        materialized='table',
        tags=['spotify', 'webapp'],
        partition_by={
            'field': 'week_start_date',
            'data_type': 'date'
        },
        cluster_by=['artist_id']
    )
}}

SELECT
    artist_id,
    week_start_date,
    listening_time_min,
    plays
FROM {{ ref('hub_music_svc__artist_listening_weekly') }}
