{{
    config(
        materialized='table',
        tags=['spotify']
    )
}}

WITH primary_artist AS (
    SELECT
        bta.track_id,
        a.artist_name
    FROM {{ ref('svc_hub__bridge_track_artist') }} AS bta
    INNER JOIN {{ ref('svc_hub__ref_artist') }} AS a ON bta.artist_id = a.artist_id
    WHERE bta.artist_position = 0
)

SELECT
    fp.played_at,
    fp.track_id,
    t.track_name,
    pa.artist_name,
    fp.album_id,
    al.album_name,
    al.album_image_url,
    t.duration_ms,
    {{
        format_duration_label(
            milliseconds_to_seconds('t.duration_ms'),
            include_seconds=true
        )
    }} AS duration_label
FROM {{ ref('svc_hub__fact_played') }} AS fp
LEFT JOIN {{ ref('svc_hub__ref_track') }} AS t ON fp.track_id = t.track_id
LEFT JOIN {{ ref('svc_hub__ref_album') }} AS al ON fp.album_id = al.album_id
LEFT JOIN primary_artist AS pa ON fp.track_id = pa.track_id
