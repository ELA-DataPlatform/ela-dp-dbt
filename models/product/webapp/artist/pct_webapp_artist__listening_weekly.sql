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

-- Dense weekly listening per artist over the last 52 full ISO weeks
-- (Europe/Paris, Monday start). Every all-time listened artist is returned even
-- when all 52 weeks are zero, so clients never rebuild weeks from daily data.

WITH week_spine AS (
    SELECT week_start_date
    FROM UNNEST(
        GENERATE_DATE_ARRAY(
            DATE_SUB(DATE_TRUNC(CURRENT_DATE('Europe/Paris'), WEEK (MONDAY)), INTERVAL 52 WEEK),
            DATE_SUB(DATE_TRUNC(CURRENT_DATE('Europe/Paris'), WEEK (MONDAY)), INTERVAL 1 WEEK),
            INTERVAL 1 WEEK
        )
    ) AS week_start_date
),

weekly_plays AS (
    SELECT
        bta.artist_id,
        DATE_TRUNC(DATE(fp.played_at, 'Europe/Paris'), WEEK (MONDAY)) AS week_start_date,
        {{ milliseconds_to_minutes('SUM(COALESCE(t.duration_ms, 0))') }} AS listening_time_min,
        COUNT(*) AS plays
    FROM {{ ref('hub_music_svc__fact_played') }} AS fp
    INNER JOIN {{ ref('hub_music_svc__bridge_track_artist') }} AS bta
        ON fp.track_id = bta.track_id AND bta.artist_position = 0
    LEFT JOIN {{ ref('hub_music_svc__ref_track') }} AS t ON fp.track_id = t.track_id
    WHERE
        DATE_TRUNC(DATE(fp.played_at, 'Europe/Paris'), WEEK (MONDAY)) BETWEEN
        (SELECT MIN(week_start_date) FROM week_spine)
        AND (SELECT MAX(week_start_date) FROM week_spine)
    GROUP BY bta.artist_id, DATE_TRUNC(DATE(fp.played_at, 'Europe/Paris'), WEEK (MONDAY))
),

all_artists AS (
    SELECT DISTINCT bta.artist_id
    FROM {{ ref('hub_music_svc__fact_played') }} AS fp
    INNER JOIN {{ ref('hub_music_svc__bridge_track_artist') }} AS bta
        ON fp.track_id = bta.track_id AND bta.artist_position = 0
),

densified AS (
    SELECT
        aa.artist_id,
        ws.week_start_date
    FROM all_artists AS aa
    CROSS JOIN week_spine AS ws
)

SELECT
    d.artist_id,
    d.week_start_date,
    COALESCE(wp.listening_time_min, 0) AS listening_time_min,
    COALESCE(wp.plays, 0) AS plays
FROM densified AS d
LEFT JOIN weekly_plays AS wp
    ON d.artist_id = wp.artist_id AND d.week_start_date = wp.week_start_date
