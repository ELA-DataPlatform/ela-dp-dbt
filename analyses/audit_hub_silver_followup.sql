-- Lecture seule PROD : impact des orphelins et comparaison vue/table Garmin.
-- Exécuter avec la même limite de facturation que audit_hub_silver.sql.
WITH orphan_track_artists AS (
    SELECT b.track_id, b.artist_position
    FROM `ela-dp-prd.hub_music_svc.bridge_track_artist` AS b
    LEFT JOIN `ela-dp-prd.hub_music_svc.ref_artist` AS a USING (artist_id)
    WHERE a.artist_id IS NULL
), primary_counts AS (
    SELECT 'track' AS entity, t.track_id AS id, COUNTIF(b.artist_position = 0) AS n
    FROM `ela-dp-prd.hub_music_svc.ref_track` AS t
    LEFT JOIN `ela-dp-prd.hub_music_svc.bridge_track_artist` AS b USING (track_id)
    GROUP BY t.track_id
    UNION ALL
    SELECT 'album', a.album_id, COUNTIF(b.artist_position = 0)
    FROM `ela-dp-prd.hub_music_svc.ref_album` AS a
    LEFT JOIN `ela-dp-prd.hub_music_svc.bridge_album_artist` AS b USING (album_id)
    GROUP BY a.album_id
), garmin_comparison AS (
    SELECT
        a.activity_id,
        TO_JSON_STRING(a.sleep) != TO_JSON_STRING(s.sleep) AS sleep_differs,
        TO_JSON_STRING(a.weather) != TO_JSON_STRING(s.weather) AS weather_differs,
        TO_JSON_STRING(a.hr_zones) != TO_JSON_STRING(s.hr_zones) AS hr_zones_differ,
        TO_JSON_STRING(a.athletic_context) != TO_JSON_STRING(s.athletic_context) AS context_differs,
        ARRAY_LENGTH(a.timeseries) != ARRAY_LENGTH(s.timeseries) AS timeseries_length_differs,
        TO_JSON_STRING(a.route) != TO_JSON_STRING(s.route) AS route_differs
    FROM `ela-dp-prd.hub_activities_svc.master_running_activities` AS a
    JOIN `ela-dp-prd.hub_activities_stg.master_running_activities` AS s USING (activity_id)
)
SELECT 'music.orphan_track_artists.distinct_artists' AS metric,
    (SELECT COUNT(DISTINCT b.artist_id)
     FROM `ela-dp-prd.hub_music_svc.bridge_track_artist` AS b
     LEFT JOIN `ela-dp-prd.hub_music_svc.ref_artist` AS a USING (artist_id)
     WHERE a.artist_id IS NULL) AS value
UNION ALL
SELECT 'music.played.with_orphan_track_artist', COUNT(*)
FROM `ela-dp-prd.hub_music_svc.fact_played` AS p
WHERE EXISTS (SELECT 1 FROM orphan_track_artists AS o WHERE o.track_id = p.track_id)
UNION ALL
SELECT 'music.played.with_missing_album', COUNT(*)
FROM `ela-dp-prd.hub_music_svc.fact_played` AS p
LEFT JOIN `ela-dp-prd.hub_music_svc.ref_album` AS a USING (album_id)
WHERE a.album_id IS NULL
UNION ALL
SELECT CONCAT('music.', entity, '.no_primary_artist'), COUNTIF(n = 0)
FROM primary_counts GROUP BY entity
UNION ALL
SELECT CONCAT('music.', entity, '.multiple_primary_artists'), COUNTIF(n > 1)
FROM primary_counts GROUP BY entity
UNION ALL
SELECT 'activities.compared_rows', COUNT(*) FROM garmin_comparison
UNION ALL
SELECT 'activities.sleep_differs', COUNTIF(sleep_differs) FROM garmin_comparison
UNION ALL
SELECT 'activities.weather_differs', COUNTIF(weather_differs) FROM garmin_comparison
UNION ALL
SELECT 'activities.hr_zones_differ', COUNTIF(hr_zones_differ) FROM garmin_comparison
UNION ALL
SELECT 'activities.context_differs', COUNTIF(context_differs) FROM garmin_comparison
UNION ALL
SELECT 'activities.timeseries_length_differs', COUNTIF(timeseries_length_differs) FROM garmin_comparison
UNION ALL
SELECT 'activities.route_differs', COUNTIF(route_differs) FROM garmin_comparison
