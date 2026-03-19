{{
    config(
        materialized='table',
        tags=['spotify']
    )
}}

WITH periods AS (
    SELECT
        period_name,
        period_start,
        period_end,
        previous_period_start,
        previous_period_end
    FROM UNNEST([
        STRUCT(
            'all_time' AS period_name,
            CAST(NULL AS TIMESTAMP) AS period_start,
            CURRENT_TIMESTAMP() AS period_end,
            CAST(NULL AS TIMESTAMP) AS previous_period_start,
            CAST(NULL AS TIMESTAMP) AS previous_period_end
        ),
        STRUCT(
            'last_365_days',
            TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 365 DAY),
            CURRENT_TIMESTAMP(),
            TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 730 DAY),
            TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 365 DAY)
        ),
        STRUCT(
            'last_30_days',
            TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY),
            CURRENT_TIMESTAMP(),
            TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 60 DAY),
            TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
        ),
        STRUCT(
            'last_7_days',
            TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY),
            CURRENT_TIMESTAMP(),
            TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 14 DAY),
            TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
        ),
        STRUCT(
            'yesterday',
            TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)),
            TIMESTAMP(CURRENT_DATE()),
            TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)),
            TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
        )
    ])
),

current_period_stats AS (
    SELECT
        p.period_name,
        fp.album_id,
        COUNT(*) AS play_count,
        SUM(t.duration_ms) AS total_listening_time_ms
    FROM {{ ref('svc_hub__fact_played') }} AS fp
    CROSS JOIN periods AS p
    INNER JOIN {{ ref('svc_hub__ref_track') }} AS t
        ON fp.track_id = t.track_id
    WHERE
        (p.period_start IS NULL OR fp.played_at >= p.period_start)
        AND fp.played_at < p.period_end
    GROUP BY p.period_name, fp.album_id
),

previous_period_stats AS (
    SELECT
        p.period_name,
        fp.album_id,
        COUNT(*) AS play_count,
        SUM(t.duration_ms) AS total_listening_time_ms
    FROM {{ ref('svc_hub__fact_played') }} AS fp
    CROSS JOIN periods AS p
    INNER JOIN {{ ref('svc_hub__ref_track') }} AS t
        ON fp.track_id = t.track_id
    WHERE
        p.previous_period_start IS NOT NULL
        AND fp.played_at >= p.previous_period_start
        AND fp.played_at < p.previous_period_end
    GROUP BY p.period_name, fp.album_id
),

current_ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY period_name
            ORDER BY total_listening_time_ms DESC
        ) AS rank
    FROM current_period_stats
),

previous_ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY period_name
            ORDER BY total_listening_time_ms DESC
        ) AS rank
    FROM previous_period_stats
),

album_primary_artist AS (
    SELECT
        baa.album_id,
        a.artist_name
    FROM {{ ref('svc_hub__bridge_album_artist') }} AS baa
    INNER JOIN {{ ref('svc_hub__ref_artist') }} AS a
        ON baa.artist_id = a.artist_id
    WHERE baa.artist_position = 0
)

SELECT
    c.period_name,
    c.rank,
    alb.album_name,
    apa.artist_name,
    alb.image_url,
    c.total_listening_time_ms,
    c.play_count,
    prev.rank AS previous_period_rank,
    FORMAT(
        '%d:%02d:%02d',
        DIV(c.total_listening_time_ms, 3600000),
        MOD(DIV(c.total_listening_time_ms, 60000), 60),
        MOD(DIV(c.total_listening_time_ms, 1000), 60)
    ) AS total_listening_time_formatted
FROM current_ranked AS c
INNER JOIN {{ ref('svc_hub__ref_album') }} AS alb
    ON c.album_id = alb.album_id
LEFT JOIN album_primary_artist AS apa
    ON c.album_id = apa.album_id
LEFT JOIN previous_ranked AS prev
    ON
        c.period_name = prev.period_name
        AND c.album_id = prev.album_id
ORDER BY c.period_name, c.rank
