{{
    config(
        materialized='table',
        tags=['spotify', 'webapp'],
        partition_by={
            'field': 'listen_date',
            'data_type': 'date'
        },
        cluster_by=['artist_id']
    )
}}

-- Presentation buckets stay in the product; shared daily measures live in the hub.
WITH with_artist_max AS (
    SELECT
        *,
        MAX(listening_time_min) OVER (PARTITION BY artist_id) AS max_listening_time_min
    FROM {{ ref('hub_music_svc__artist_listening_daily') }}
)

SELECT
    artist_id,
    listen_date,
    listening_time_min,
    plays,
    CASE
        WHEN listening_time_min <= 0 THEN 0
        WHEN SAFE_DIVIDE(listening_time_min, max_listening_time_min) < 0.20 THEN 1
        WHEN SAFE_DIVIDE(listening_time_min, max_listening_time_min) < 0.45 THEN 2
        WHEN SAFE_DIVIDE(listening_time_min, max_listening_time_min) < 0.75 THEN 3
        ELSE 4
    END AS heatmap_level
FROM with_artist_max
