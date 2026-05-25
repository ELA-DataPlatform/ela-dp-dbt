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

-- Sparse daily listening per artist over the last 365 days (Europe/Paris):
-- one row only for days that had listening. The frontend fills empty days with
-- zeros (GitHub-style heatmap). Plays are credited to the primary artist of each
-- track. listening_time_min approximates time with full track duration.
-- Window driven by stg_hub__ref_calendar period '1y'.

WITH cal AS (
    SELECT period_start
    FROM {{ ref('stg_hub__ref_calendar') }}
    WHERE period = '1y'
)

SELECT
    bta.artist_id,
    DATE(fp.played_at, 'Europe/Paris') AS listen_date,
    CAST(SUM(COALESCE(t.duration_ms, 0)) / 60000 AS INT64) AS listening_time_min,
    COUNT(*) AS plays
FROM {{ ref('svc_hub__fact_played') }} AS fp
INNER JOIN {{ ref('svc_hub__bridge_track_artist') }} AS bta
    ON fp.track_id = bta.track_id AND bta.artist_position = 0
LEFT JOIN {{ ref('svc_hub__ref_track') }} AS t ON fp.track_id = t.track_id
WHERE
    DATE(fp.played_at, 'Europe/Paris') >= (SELECT period_start FROM cal)
GROUP BY bta.artist_id, DATE(fp.played_at, 'Europe/Paris')
