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
        fp.album_id,
        t.duration_ms
    FROM {{ ref('svc_hub__fact_played') }} AS fp
    LEFT JOIN {{ ref('svc_hub__ref_track') }} AS t ON fp.track_id = t.track_id
),

plays_by_period AS (
    SELECT
        track_id,
        album_id,
        COUNT(*) AS play_count,
        SUM(duration_ms) AS total_duration_ms,
        'today' AS period
    FROM fact
    WHERE DATE(played_at) = CURRENT_DATE()
    GROUP BY
        track_id,
        album_id

    UNION ALL

    SELECT
        track_id,
        album_id,
        COUNT(*) AS play_count,
        SUM(duration_ms) AS total_duration_ms,
        'yesterday' AS period
    FROM fact
    WHERE DATE(played_at) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    GROUP BY
        track_id,
        album_id

    UNION ALL

    SELECT
        track_id,
        album_id,
        COUNT(*) AS play_count,
        SUM(duration_ms) AS total_duration_ms,
        'last_7_days' AS period
    FROM fact
    WHERE played_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
    GROUP BY
        track_id,
        album_id

    UNION ALL

    SELECT
        track_id,
        album_id,
        COUNT(*) AS play_count,
        SUM(duration_ms) AS total_duration_ms,
        'last_30_days' AS period
    FROM fact
    WHERE played_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
    GROUP BY
        track_id,
        album_id

    UNION ALL

    SELECT
        track_id,
        album_id,
        COUNT(*) AS play_count,
        SUM(duration_ms) AS total_duration_ms,
        'last_6_months' AS period
    FROM fact
    WHERE played_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 180 DAY)
    GROUP BY
        track_id,
        album_id

    UNION ALL

    SELECT
        track_id,
        album_id,
        COUNT(*) AS play_count,
        SUM(duration_ms) AS total_duration_ms,
        'all_time' AS period
    FROM fact
    GROUP BY
        track_id,
        album_id
),

ranked AS (
    SELECT
        period,
        track_id,
        album_id,
        play_count,
        total_duration_ms,
        ROW_NUMBER() OVER (PARTITION BY period ORDER BY total_duration_ms DESC) AS rank
    FROM plays_by_period
),

artists_agg AS (
    SELECT
        bta.track_id,
        STRING_AGG(ra.artist_name ORDER BY bta.artist_position) AS artists
    FROM {{ ref('svc_hub__bridge_track_artist') }} AS bta
    LEFT JOIN {{ ref('svc_hub__ref_artist') }} AS ra ON bta.artist_id = ra.artist_id
    GROUP BY bta.track_id
)

SELECT
    r.period,
    r.rank,
    r.track_id,
    t.track_name,
    r.album_id,
    al.album_name,
    aa.artists,
    r.play_count,
    r.total_duration_ms,
    CONCAT(
        CAST(DIV(r.total_duration_ms, 3600000) AS STRING), 'h ',
        CAST(DIV(MOD(r.total_duration_ms, 3600000), 60000) AS STRING), 'm'
    ) AS total_duration_formatted,
    t.popularity
FROM ranked AS r
LEFT JOIN {{ ref('svc_hub__ref_track') }} AS t ON r.track_id = t.track_id
LEFT JOIN {{ ref('svc_hub__ref_album') }} AS al ON r.album_id = al.album_id
LEFT JOIN artists_agg AS aa ON r.track_id = aa.track_id
WHERE r.rank <= 20
ORDER BY
    r.period,
    r.rank
