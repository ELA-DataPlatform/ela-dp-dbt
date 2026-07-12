{{
    config(
        materialized='incremental',
        unique_key=['activity_id', 'played_at'],
        incremental_strategy='merge',
        tags=['garmin', 'webapp', 'spotify']
    )
}}

WITH activities AS (
    SELECT
        act.activity_id,
        act.activity_date,
        act.start_time_gmt,
        act.end_time_gmt,
        act.timeseries,
        act._ingested_at
    FROM {{ ref('svc_hub__master_running_activities') }} AS act
    WHERE act.end_time_gmt IS NOT NULL
),

track_artist_ranked AS (
    SELECT
        bta.track_id,
        ar.artist_id,
        ar.artist_name,
        ROW_NUMBER() OVER (
            PARTITION BY bta.track_id
            ORDER BY ar.artist_id
        ) AS rn
    FROM {{ ref('svc_hub__bridge_track_artist') }} AS bta
    INNER JOIN {{ ref('svc_hub__ref_artist') }} AS ar
        ON bta.artist_id = ar.artist_id
),

track_artist AS (
    SELECT
        tar.track_id,
        tar.artist_id,
        tar.artist_name
    FROM track_artist_ranked AS tar
    WHERE tar.rn = 1
),

played_during_activity AS (
    SELECT
        a.activity_id,
        a.activity_date,
        a._ingested_at,
        fp.played_at,
        fp.track_id,
        t.track_name,
        t.duration_ms,
        al.album_id,
        al.album_name,
        ta.artist_id,
        ta.artist_name,
        al.album_image_url,
        (
            SELECT {{ meters_to_kilometers('ts.cum_distance_m', 2) }}
            FROM UNNEST(a.timeseries) AS ts
            WHERE
                ts.timestamp_gmt <= fp.played_at
                AND ts.timestamp_gmt > TIMESTAMP('1971-01-01')
            ORDER BY ts.timestamp_gmt DESC
            LIMIT 1
        ) AS km_start,
        (
            SELECT ts.latitude
            FROM UNNEST(a.timeseries) AS ts
            WHERE
                ts.timestamp_gmt <= fp.played_at
                AND ts.timestamp_gmt > TIMESTAMP('1971-01-01')
                AND ts.latitude IS NOT NULL
            ORDER BY ts.timestamp_gmt DESC
            LIMIT 1
        ) AS latitude,
        (
            SELECT ts.longitude
            FROM UNNEST(a.timeseries) AS ts
            WHERE
                ts.timestamp_gmt <= fp.played_at
                AND ts.timestamp_gmt > TIMESTAMP('1971-01-01')
                AND ts.longitude IS NOT NULL
            ORDER BY ts.timestamp_gmt DESC
            LIMIT 1
        ) AS longitude
    FROM activities AS a
    INNER JOIN {{ ref('svc_hub__fact_played') }} AS fp
        ON fp.played_at BETWEEN a.start_time_gmt AND a.end_time_gmt
    INNER JOIN {{ ref('svc_hub__ref_track') }} AS t
        ON fp.track_id = t.track_id
    INNER JOIN {{ ref('svc_hub__ref_album') }} AS al
        ON fp.album_id = al.album_id
    LEFT JOIN track_artist AS ta
        ON fp.track_id = ta.track_id
)

SELECT
    pda.activity_id,
    pda.activity_date,
    pda.played_at,
    pda.track_id,
    pda.track_name,
    pda.artist_id,
    pda.artist_name,
    pda.album_id,
    pda.album_name,
    pda.album_image_url,
    pda.km_start,
    pda.latitude,
    pda.longitude,
    pda._ingested_at,
    {{ milliseconds_to_seconds('pda.duration_ms') }} AS duration_seconds
FROM played_during_activity AS pda

{% if is_incremental() %}
    WHERE pda._ingested_at > (SELECT MAX(_ingested_at) FROM {{ this }})
{% endif %}

ORDER BY pda.activity_id, pda.played_at
