{{
    config(
        materialized='table',
        tags=['spotify']
    )
}}

WITH fact AS (
    SELECT
        fp.played_at,
        fp.track_id,
        t.duration_ms
    FROM {{ ref('svc_hub__fact_played') }} AS fp
    LEFT JOIN {{ ref('svc_hub__ref_track') }} AS t ON fp.track_id = t.track_id
),

fact_with_artist AS (
    SELECT
        f.played_at,
        f.duration_ms,
        bta.artist_id
    FROM fact AS f
    INNER JOIN {{ ref('svc_hub__bridge_track_artist') }} AS bta ON f.track_id = bta.track_id
),

plays_by_period AS (
    SELECT
        artist_id,
        COUNT(*) AS play_count,
        SUM(duration_ms) AS total_duration_ms,
        'today' AS period
    FROM fact_with_artist
    WHERE DATE(played_at) = CURRENT_DATE()
    GROUP BY artist_id

    UNION ALL

    SELECT
        artist_id,
        COUNT(*) AS play_count,
        SUM(duration_ms) AS total_duration_ms,
        'yesterday' AS period
    FROM fact_with_artist
    WHERE DATE(played_at) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    GROUP BY artist_id

    UNION ALL

    SELECT
        artist_id,
        COUNT(*) AS play_count,
        SUM(duration_ms) AS total_duration_ms,
        'last_7_days' AS period
    FROM fact_with_artist
    WHERE played_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
    GROUP BY artist_id

    UNION ALL

    SELECT
        artist_id,
        COUNT(*) AS play_count,
        SUM(duration_ms) AS total_duration_ms,
        'last_30_days' AS period
    FROM fact_with_artist
    WHERE played_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
    GROUP BY artist_id

    UNION ALL

    SELECT
        artist_id,
        COUNT(*) AS play_count,
        SUM(duration_ms) AS total_duration_ms,
        'last_6_months' AS period
    FROM fact_with_artist
    WHERE played_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 180 DAY)
    GROUP BY artist_id

    UNION ALL

    SELECT
        artist_id,
        COUNT(*) AS play_count,
        SUM(duration_ms) AS total_duration_ms,
        'all_time' AS period
    FROM fact_with_artist
    GROUP BY artist_id
),

ranked AS (
    SELECT
        period,
        artist_id,
        play_count,
        total_duration_ms,
        ROW_NUMBER() OVER (PARTITION BY period ORDER BY total_duration_ms DESC) AS rank
    FROM plays_by_period
)

SELECT
    r.period,
    r.rank,
    r.artist_id,
    a.artist_name,
    a.genres,
    a.followers_total,
    a.popularity,
    r.play_count,
    r.total_duration_ms,
    CONCAT(
        CAST(DIV(r.total_duration_ms, 3600000) AS STRING), 'h ',
        CAST(DIV(MOD(r.total_duration_ms, 3600000), 60000) AS STRING), 'm'
    ) AS total_duration_formatted
FROM ranked AS r
LEFT JOIN {{ ref('svc_hub__ref_artist') }} AS a ON r.artist_id = a.artist_id
WHERE r.rank <= 20
ORDER BY
    r.period,
    r.rank
