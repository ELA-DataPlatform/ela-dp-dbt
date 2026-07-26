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

-- Dense daily listening per artist over the last 365 calendar days
-- (Europe/Paris). Every artist/day pair is present, including zero-listening
-- days, so clients never have to infer the date window or fill gaps.

WITH date_spine AS (
    SELECT listen_date
    FROM UNNEST(
        GENERATE_DATE_ARRAY(
            DATE_SUB(CURRENT_DATE('Europe/Paris'), INTERVAL 364 DAY),
            CURRENT_DATE('Europe/Paris')
        )
    ) AS listen_date
),

artists AS (
    SELECT DISTINCT bta.artist_id
    FROM {{ ref('svc_hub__fact_played') }} AS fp
    INNER JOIN {{ ref('svc_hub__bridge_track_artist') }} AS bta
        ON fp.track_id = bta.track_id AND bta.artist_position = 0
),

daily_plays AS (
    SELECT
        bta.artist_id,
        DATE(fp.played_at, 'Europe/Paris') AS listen_date,
        {{ milliseconds_to_minutes('SUM(COALESCE(t.duration_ms, 0))') }} AS listening_time_min,
        COUNT(*) AS plays
    FROM {{ ref('svc_hub__fact_played') }} AS fp
    INNER JOIN {{ ref('svc_hub__bridge_track_artist') }} AS bta
        ON fp.track_id = bta.track_id AND bta.artist_position = 0
    LEFT JOIN {{ ref('svc_hub__ref_track') }} AS t ON fp.track_id = t.track_id
    WHERE
        DATE(fp.played_at, 'Europe/Paris')
        BETWEEN DATE_SUB(CURRENT_DATE('Europe/Paris'), INTERVAL 364 DAY)
        AND CURRENT_DATE('Europe/Paris')
    GROUP BY bta.artist_id, DATE(fp.played_at, 'Europe/Paris')
),

densified AS (
    SELECT
        a.artist_id,
        d.listen_date,
        COALESCE(dp.listening_time_min, 0) AS listening_time_min,
        COALESCE(dp.plays, 0) AS plays
    FROM artists AS a
    CROSS JOIN date_spine AS d
    LEFT JOIN daily_plays AS dp
        ON a.artist_id = dp.artist_id AND d.listen_date = dp.listen_date
),

with_artist_max AS (
    SELECT
        *,
        MAX(listening_time_min) OVER (PARTITION BY artist_id) AS max_listening_time_min
    FROM densified
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
