{{
    config(
        materialized='table',
        tags=['spotify']
    )
}}

WITH fact_with_artist AS (
    SELECT
        fp.played_at,
        fp.track_id,
        t.duration_ms,
        bta.artist_id,
        DATE(fp.played_at, 'Europe/Paris') AS play_date
    FROM {{ ref('hub_music_svc__fact_played') }} AS fp
    LEFT JOIN {{ ref('hub_music_svc__ref_track') }} AS t ON fp.track_id = t.track_id
    INNER JOIN {{ ref('hub_music_svc__bridge_track_artist') }} AS bta
        ON fp.track_id = bta.track_id AND bta.artist_position = 0
),

current_agg AS (
    SELECT
        fa.artist_id,
        cp.period,
        COUNT(*) AS play_count,
        {{ milliseconds_to_minutes('SUM(fa.duration_ms)') }} AS listening_time_min,
        COUNT(DISTINCT fa.track_id) AS unique_tracks,
        MAX(fa.played_at) AS last_played_at
    FROM fact_with_artist AS fa
    CROSS JOIN {{ ref('hub_utils_stg__ref_calendar') }} AS cp
    WHERE
        (cp.period_start IS NULL OR fa.play_date >= cp.period_start)
        AND fa.play_date <= cp.period_end
    GROUP BY fa.artist_id, cp.period
),

prev_agg AS (
    SELECT
        fa.artist_id,
        cp.period,
        {{ milliseconds_to_minutes('SUM(fa.duration_ms)') }} AS listening_time_min
    FROM fact_with_artist AS fa
    CROSS JOIN {{ ref('hub_utils_stg__ref_calendar') }} AS cp
    WHERE
        cp.prev_period_start IS NOT NULL
        AND fa.play_date >= cp.prev_period_start
        AND fa.play_date <= cp.prev_period_end
    GROUP BY fa.artist_id, cp.period
),

current_ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY period
            ORDER BY listening_time_min DESC
        ) AS rank
    FROM current_agg
),

prev_ranked AS (
    SELECT
        artist_id,
        period,
        ROW_NUMBER() OVER (
            PARTITION BY period
            ORDER BY listening_time_min DESC
        ) AS rank_previous
    FROM prev_agg
)

SELECT
    c.period,
    c.rank,
    pr.rank_previous,
    c.artist_id,
    a.artist_name,
    a.artist_image_url,
    c.play_count,
    c.listening_time_min,
    c.unique_tracks,
    c.last_played_at,
    (pr.rank_previous IS NULL AND c.period != 'all') AS is_new_entry,
    CASE
        WHEN pr.rank_previous IS NULL AND c.period != 'all' THEN 'new'
        WHEN pr.rank_previous IS NULL THEN 'none'
        WHEN pr.rank_previous > c.rank THEN 'up'
        WHEN pr.rank_previous < c.rank THEN 'down'
        ELSE 'same'
    END AS rank_change_direction,
    COALESCE(ABS(pr.rank_previous - c.rank), 0) AS rank_change_places
FROM current_ranked AS c
LEFT JOIN prev_ranked AS pr
    ON c.artist_id = pr.artist_id AND c.period = pr.period
LEFT JOIN {{ ref('hub_music_svc__ref_artist') }} AS a ON c.artist_id = a.artist_id
WHERE c.rank <= 20
ORDER BY c.period, c.rank
