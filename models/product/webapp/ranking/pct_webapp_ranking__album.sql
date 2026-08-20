{{
    config(
        materialized='table',
        tags=['spotify']
    )
}}

WITH fact_with_album AS (
    SELECT
        fp.played_at,
        fp.track_id,
        fp.album_id,
        t.duration_ms,
        DATE(fp.played_at, 'Europe/Paris') AS play_date
    FROM {{ ref('hub_music_svc__fact_played') }} AS fp
    LEFT JOIN {{ ref('hub_music_svc__ref_track') }} AS t ON fp.track_id = t.track_id
    WHERE fp.album_id IS NOT NULL
),

current_agg AS (
    SELECT
        fa.album_id,
        cp.period,
        COUNT(*) AS play_count,
        {{ milliseconds_to_minutes('SUM(fa.duration_ms)') }} AS listening_time_min,
        COUNT(DISTINCT fa.track_id) AS unique_tracks,
        MAX(fa.played_at) AS last_played_at
    FROM fact_with_album AS fa
    CROSS JOIN {{ ref('hub_utils_stg__ref_calendar') }} AS cp
    WHERE
        (cp.period_start IS NULL OR fa.play_date >= cp.period_start)
        AND fa.play_date <= cp.period_end
    GROUP BY fa.album_id, cp.period
),

prev_agg AS (
    SELECT
        fa.album_id,
        cp.period,
        {{ milliseconds_to_minutes('SUM(fa.duration_ms)') }} AS listening_time_min
    FROM fact_with_album AS fa
    CROSS JOIN {{ ref('hub_utils_stg__ref_calendar') }} AS cp
    WHERE
        cp.prev_period_start IS NOT NULL
        AND fa.play_date >= cp.prev_period_start
        AND fa.play_date <= cp.prev_period_end
    GROUP BY fa.album_id, cp.period
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
        album_id,
        period,
        ROW_NUMBER() OVER (
            PARTITION BY period
            ORDER BY listening_time_min DESC
        ) AS rank_previous
    FROM prev_agg
),

primary_artist AS (
    SELECT
        baa.album_id,
        a.artist_id,
        a.artist_name
    FROM {{ ref('hub_music_svc__bridge_album_artist') }} AS baa
    INNER JOIN {{ ref('hub_music_svc__ref_artist') }} AS a ON baa.artist_id = a.artist_id
    WHERE baa.artist_position = 0
)

SELECT
    c.period,
    c.rank,
    pr.rank_previous,
    c.album_id,
    al.album_name,
    pa.artist_id,
    pa.artist_name,
    al.album_image_url,
    c.play_count,
    c.listening_time_min,
    c.unique_tracks,
    c.last_played_at,
    al.total_tracks,
    (pr.rank_previous IS NULL AND c.period != 'all') AS is_new_entry,
    CASE
        WHEN pr.rank_previous IS NULL AND c.period != 'all' THEN 'new'
        WHEN pr.rank_previous IS NULL THEN 'none'
        WHEN pr.rank_previous > c.rank THEN 'up'
        WHEN pr.rank_previous < c.rank THEN 'down'
        ELSE 'same'
    END AS rank_change_direction,
    COALESCE(ABS(pr.rank_previous - c.rank), 0) AS rank_change_places,
    CAST(SUBSTR(al.release_date, 1, 4) AS INT64) AS release_year
FROM current_ranked AS c
LEFT JOIN prev_ranked AS pr
    ON c.album_id = pr.album_id AND c.period = pr.period
LEFT JOIN {{ ref('hub_music_svc__ref_album') }} AS al ON c.album_id = al.album_id
LEFT JOIN primary_artist AS pa ON c.album_id = pa.album_id
WHERE c.rank <= 20
ORDER BY c.period, c.rank
