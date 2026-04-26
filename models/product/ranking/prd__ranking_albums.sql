{{
    config(
        materialized='table',
        tags=['spotify']
    )
}}

WITH fact AS (
    SELECT
        fp.played_at,
        fp.album_id,
        t.duration_ms
    FROM {{ ref('svc_hub__fact_played') }} AS fp
    LEFT JOIN {{ ref('svc_hub__ref_track') }} AS t ON fp.track_id = t.track_id
),

plays_by_period AS (
    SELECT
        album_id,
        COUNT(*) AS play_count,
        SUM(duration_ms) AS total_duration_ms,
        'today' AS period
    FROM fact
    WHERE DATE(played_at) = CURRENT_DATE()
    GROUP BY album_id

    UNION ALL

    SELECT
        album_id,
        COUNT(*) AS play_count,
        SUM(duration_ms) AS total_duration_ms,
        'yesterday' AS period
    FROM fact
    WHERE DATE(played_at) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    GROUP BY album_id

    UNION ALL

    SELECT
        album_id,
        COUNT(*) AS play_count,
        SUM(duration_ms) AS total_duration_ms,
        'last_7_days' AS period
    FROM fact
    WHERE played_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
    GROUP BY album_id

    UNION ALL

    SELECT
        album_id,
        COUNT(*) AS play_count,
        SUM(duration_ms) AS total_duration_ms,
        'last_30_days' AS period
    FROM fact
    WHERE played_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
    GROUP BY album_id

    UNION ALL

    SELECT
        album_id,
        COUNT(*) AS play_count,
        SUM(duration_ms) AS total_duration_ms,
        'last_6_months' AS period
    FROM fact
    WHERE played_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 180 DAY)
    GROUP BY album_id

    UNION ALL

    SELECT
        album_id,
        COUNT(*) AS play_count,
        SUM(duration_ms) AS total_duration_ms,
        'all_time' AS period
    FROM fact
    GROUP BY album_id
),

ranked AS (
    SELECT
        period,
        album_id,
        play_count,
        total_duration_ms,
        ROW_NUMBER() OVER (PARTITION BY period ORDER BY total_duration_ms DESC) AS rank
    FROM plays_by_period
),

artists_agg AS (
    SELECT
        baa.album_id,
        STRING_AGG(ra.artist_name ORDER BY baa.artist_position) AS artists
    FROM {{ ref('svc_hub__bridge_album_artist') }} AS baa
    LEFT JOIN {{ ref('svc_hub__ref_artist') }} AS ra ON baa.artist_id = ra.artist_id
    GROUP BY baa.album_id
)

SELECT
    r.period,
    r.rank,
    r.album_id,
    al.album_name,
    al.album_type,
    al.release_date,
    aa.artists,
    r.play_count,
    r.total_duration_ms,
    CONCAT(
        CAST(DIV(r.total_duration_ms, 3600000) AS STRING), 'h ',
        CAST(DIV(MOD(r.total_duration_ms, 3600000), 60000) AS STRING), 'm'
    ) AS total_duration_formatted,
    al.popularity
FROM ranked AS r
LEFT JOIN {{ ref('svc_hub__ref_album') }} AS al ON r.album_id = al.album_id
LEFT JOIN artists_agg AS aa ON r.album_id = aa.album_id
WHERE r.rank <= 20
ORDER BY
    r.period,
    r.rank
