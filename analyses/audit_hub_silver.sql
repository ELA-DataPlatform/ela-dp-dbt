-- Audit read-only du silver PROD. Aucun DDL/DML, résultats agrégés uniquement.
-- Exécution : bq query --project_id=ela-dp-prd --location=EU \
--   --use_legacy_sql=false --maximum_bytes_billed=1073741824 --format=json \
--   < analyses/audit_hub_silver.sql
WITH checks AS (
    SELECT 'music.played' AS entity, COUNT(*) AS total_rows,
        COUNTIF(played_at IS NULL OR track_id IS NULL) AS invalid_keys,
        COUNT(*) - COUNT(DISTINCT TO_JSON_STRING(STRUCT(played_at, track_id))) AS duplicate_keys,
        MAX(_ingested_at) AS last_ingested_at
    FROM `ela-dp-prd.hub_music_svc.fact_played`
    UNION ALL
    SELECT 'music.track', COUNT(*), COUNTIF(track_id IS NULL),
        COUNT(*) - COUNT(DISTINCT TO_JSON_STRING(STRUCT(track_id))), MAX(_ingested_at)
    FROM `ela-dp-prd.hub_music_svc.ref_track`
    UNION ALL
    SELECT 'music.album', COUNT(*), COUNTIF(album_id IS NULL),
        COUNT(*) - COUNT(DISTINCT TO_JSON_STRING(STRUCT(album_id))), MAX(_ingested_at)
    FROM `ela-dp-prd.hub_music_svc.ref_album`
    UNION ALL
    SELECT 'music.artist', COUNT(*), COUNTIF(artist_id IS NULL),
        COUNT(*) - COUNT(DISTINCT TO_JSON_STRING(STRUCT(artist_id))), MAX(_ingested_at)
    FROM `ela-dp-prd.hub_music_svc.ref_artist`
    UNION ALL
    SELECT 'activities.running', COUNT(*), COUNTIF(activity_id IS NULL),
        COUNT(*) - COUNT(DISTINCT TO_JSON_STRING(STRUCT(activity_id))), MAX(_ingested_at)
    FROM `ela-dp-prd.hub_activities_svc.master_running_activities`
), metrics AS (
    SELECT entity, 'invalid_keys' AS check_name, invalid_keys AS affected, total_rows AS denominator FROM checks
    UNION ALL
    SELECT entity, 'duplicate_keys', duplicate_keys, total_rows FROM checks
    UNION ALL
    SELECT 'music.played', 'missing_track', COUNTIF(t.track_id IS NULL), COUNT(*)
    FROM `ela-dp-prd.hub_music_svc.fact_played` AS p
    LEFT JOIN `ela-dp-prd.hub_music_svc.ref_track` AS t USING (track_id)
    UNION ALL
    SELECT 'music.track', 'missing_album', COUNTIF(a.album_id IS NULL), COUNT(*)
    FROM `ela-dp-prd.hub_music_svc.ref_track` AS t
    LEFT JOIN `ela-dp-prd.hub_music_svc.ref_album` AS a USING (album_id)
    UNION ALL
    SELECT 'music.track', 'missing_or_nonpositive_duration', COUNTIF(duration_ms IS NULL OR duration_ms <= 0), COUNT(*)
    FROM `ela-dp-prd.hub_music_svc.ref_track`
    UNION ALL
    SELECT 'music.track_artist', 'missing_artist', COUNTIF(a.artist_id IS NULL), COUNT(*)
    FROM `ela-dp-prd.hub_music_svc.bridge_track_artist` AS b
    LEFT JOIN `ela-dp-prd.hub_music_svc.ref_artist` AS a USING (artist_id)
    UNION ALL
    SELECT 'music.album_artist', 'missing_artist', COUNTIF(a.artist_id IS NULL), COUNT(*)
    FROM `ela-dp-prd.hub_music_svc.bridge_album_artist` AS b
    LEFT JOIN `ela-dp-prd.hub_music_svc.ref_artist` AS a USING (artist_id)
    UNION ALL
    SELECT 'music.track', 'primary_artist_count_not_one', COUNTIF(n != 1), COUNT(*)
    FROM (
        SELECT t.track_id, COUNTIF(b.artist_position = 0) AS n
        FROM `ela-dp-prd.hub_music_svc.ref_track` AS t
        LEFT JOIN `ela-dp-prd.hub_music_svc.bridge_track_artist` AS b USING (track_id)
        GROUP BY t.track_id
    )
    UNION ALL
    SELECT 'music.album', 'primary_artist_count_not_one', COUNTIF(n != 1), COUNT(*)
    FROM (
        SELECT a.album_id, COUNTIF(b.artist_position = 0) AS n
        FROM `ela-dp-prd.hub_music_svc.ref_album` AS a
        LEFT JOIN `ela-dp-prd.hub_music_svc.bridge_album_artist` AS b USING (album_id)
        GROUP BY a.album_id
    )
    UNION ALL
    SELECT 'activities.running', 'negative_distance_or_duration',
        COUNTIF(performance.distance_m < 0 OR performance.duration_s < 0), COUNT(*)
    FROM `ela-dp-prd.hub_activities_svc.master_running_activities`
    UNION ALL
    SELECT 'activities.running', 'missing_timeseries', COUNTIF(ARRAY_LENGTH(timeseries) = 0), COUNT(*)
    FROM `ela-dp-prd.hub_activities_svc.master_running_activities`
    UNION ALL
    SELECT 'activities.running', 'details_newer_than_master', COUNTIF(d._ingested_at > a._ingested_at), COUNT(*)
    FROM `ela-dp-prd.hub_activities_svc.master_running_activities` AS a
    LEFT JOIN `ela-dp-prd.lake_garmin_svc.activity_details` AS d ON a.activity_id = d.activityid
)
SELECT m.*, SAFE_DIVIDE(affected, denominator) AS affected_rate, c.last_ingested_at
FROM metrics AS m
LEFT JOIN checks AS c USING (entity)
ORDER BY entity, check_name
