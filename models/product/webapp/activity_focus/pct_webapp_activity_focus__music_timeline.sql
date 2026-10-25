{{
    config(
        materialized='table',
        tags=['garmin', 'webapp', 'spotify']
    )
}}

SELECT
    activity_id,
    activity_date,
    played_at,
    track_id,
    track_name,
    artist_id,
    artist_name,
    album_id,
    album_name,
    album_image_url,
    km_start,
    latitude,
    longitude,
    _ingested_at,
    duration_seconds
FROM {{ ref('hub_activities_svc__music_timeline') }}
ORDER BY activity_id, played_at
