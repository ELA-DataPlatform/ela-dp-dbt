{{ config(materialized='view') }}

-- One Garmin account-level night per wake-up date. Date D represents the night D-1 to D; no identity is invented
-- when the source lacks a profile ID.
WITH normalized AS (
    SELECT
        s.date AS health_date,
        CAST(NULL AS STRING) AS source_user_id,
        'account' AS identity_scope,
        'garmin' AS source_system,
        'sleep' AS source_table,
        s._ingested_at,
        SAFE_CAST(s.sleep_calendar_date AS DATE) AS sleep_calendar_date,
        s.sleep_overall_score AS sleep_score,
        s.sleep_overall_score_qualifier AS score_qualifier,
        s.sleep_quality_key AS quality_key,
        s.sleep_score_feedback AS score_feedback,
        s.sleep_time_seconds AS total_sleep_s,
        s.deep_sleep_seconds AS deep_sleep_s,
        s.light_sleep_seconds AS light_sleep_s,
        s.rem_sleep_seconds AS rem_sleep_s,
        s.awake_sleep_seconds AS awake_sleep_s,
        s.unmeasurable_sleep_seconds AS unmeasurable_sleep_s,
        s.nap_time_seconds AS nap_s,
        s.avgovernighthrv AS avg_overnight_hrv_ms,
        s.hrvstatus AS hrv_status,
        s.restingheartrate AS resting_hr_bpm,
        s.bodybatterychange AS body_battery_change,
        s.sleep_avg_spo2_value AS avg_spo2_pct,
        s.sleep_lowest_spo2_value AS lowest_spo2_pct,
        s.sleep_avg_respiration AS avg_respiration_rpm,
        s.sleep_lowest_respiration AS lowest_respiration_rpm,
        s.sleep_highest_respiration AS highest_respiration_rpm,
        s.sleep_restless_moments_count AS restless_moments,
        {{ garmin_health_timestamp('s.sleep_start_timestamp_gmt') }} AS sleep_start_at_utc,
        {{ garmin_health_timestamp('s.sleep_end_timestamp_gmt') }} AS sleep_end_at_utc,
        DATETIME({{ garmin_health_timestamp('s.sleep_start_timestamp_local') }}) AS sleep_start_at_local,
        DATETIME({{ garmin_health_timestamp('s.sleep_end_timestamp_local') }}) AS sleep_end_at_local,
        (
            SELECT v.value
            FROM UNNEST(s.sleep_body_battery) AS v
            WHERE v.value BETWEEN 0 AND 100
            ORDER BY v.start_gmt ASC
            LIMIT 1
        ) AS body_battery_at_sleep,
        (
            SELECT v.value
            FROM UNNEST(s.sleep_body_battery) AS v
            WHERE v.value BETWEEN 0 AND 100
            ORDER BY v.start_gmt DESC
            LIMIT 1
        ) AS body_battery_at_wake,
        s.sleep_start_timestamp_gmt AS sleep_start_source_value,
        s.sleep_end_timestamp_gmt AS sleep_end_source_value,
        s.sleep_start_timestamp_local AS sleep_start_local_source_value,
        s.sleep_end_timestamp_local AS sleep_end_local_source_value,
        TO_JSON_STRING(STRUCT(s.date AS source_date, CAST(NULL AS STRING) AS source_user_id)) AS source_record_key
    FROM {{ ref('dlk_garmin_svc__sleep') }} AS s
),

qualified AS (
    SELECT
        *,
        ARRAY_CONCAT(
            IF(health_date IS NULL, ['missing_health_date'], ARRAY<STRING>[]),
            IF(
                sleep_score IS NOT NULL AND NOT (sleep_score BETWEEN 0 AND 100),
                ['invalid_sleep_score'],
                ARRAY<STRING>[]
            ),
            IF(total_sleep_s IS NOT NULL AND NOT (total_sleep_s >= 0), ['invalid_total_sleep_s'], ARRAY<STRING>[]),
            IF(deep_sleep_s IS NOT NULL AND NOT (deep_sleep_s >= 0), ['invalid_deep_sleep_s'], ARRAY<STRING>[]),
            IF(light_sleep_s IS NOT NULL AND NOT (light_sleep_s >= 0), ['invalid_light_sleep_s'], ARRAY<STRING>[]),
            IF(rem_sleep_s IS NOT NULL AND NOT (rem_sleep_s >= 0), ['invalid_rem_sleep_s'], ARRAY<STRING>[]),
            IF(awake_sleep_s IS NOT NULL AND NOT (awake_sleep_s >= 0), ['invalid_awake_sleep_s'], ARRAY<STRING>[]),
            IF(
                unmeasurable_sleep_s IS NOT NULL AND NOT (unmeasurable_sleep_s >= 0),
                ['invalid_unmeasurable_sleep_s'],
                ARRAY<STRING>[]
            ),
            IF(nap_s IS NOT NULL AND NOT (nap_s >= 0), ['invalid_nap_s'], ARRAY<STRING>[]),
            IF(
                avg_overnight_hrv_ms IS NOT NULL AND NOT (avg_overnight_hrv_ms > 0),
                ['invalid_avg_overnight_hrv_ms'],
                ARRAY<STRING>[]
            ),
            IF(resting_hr_bpm IS NOT NULL AND NOT (resting_hr_bpm > 0), ['invalid_resting_hr_bpm'], ARRAY<STRING>[]),
            IF(
                avg_spo2_pct IS NOT NULL AND NOT (avg_spo2_pct > 0 AND avg_spo2_pct <= 100),
                ['invalid_avg_spo2_pct'],
                ARRAY<STRING>[]
            ),
            IF(
                lowest_spo2_pct IS NOT NULL AND NOT (lowest_spo2_pct > 0 AND lowest_spo2_pct <= 100),
                ['invalid_lowest_spo2_pct'],
                ARRAY<STRING>[]
            ),
            IF(
                avg_respiration_rpm IS NOT NULL AND NOT (avg_respiration_rpm > 0),
                ['invalid_avg_respiration_rpm'],
                ARRAY<STRING>[]
            ),
            IF(
                lowest_respiration_rpm IS NOT NULL AND NOT (lowest_respiration_rpm > 0),
                ['invalid_lowest_respiration_rpm'],
                ARRAY<STRING>[]
            ),
            IF(
                highest_respiration_rpm IS NOT NULL AND NOT (highest_respiration_rpm > 0),
                ['invalid_highest_respiration_rpm'],
                ARRAY<STRING>[]
            ),
            IF(
                restless_moments IS NOT NULL AND NOT (restless_moments >= 0),
                ['invalid_restless_moments'],
                ARRAY<STRING>[]
            ),
            IF(
                body_battery_at_sleep IS NOT NULL AND NOT (body_battery_at_sleep BETWEEN 0 AND 100),
                ['invalid_body_battery_at_sleep'],
                ARRAY<STRING>[]
            ),
            IF(
                body_battery_at_wake IS NOT NULL AND NOT (body_battery_at_wake BETWEEN 0 AND 100),
                ['invalid_body_battery_at_wake'],
                ARRAY<STRING>[]
            ),
            IF(
                sleep_start_source_value IS NOT NULL AND sleep_start_at_utc IS NULL,
                ['invalid_sleep_start_timestamp'],
                ARRAY<STRING>[]
            ),
            IF(
                sleep_end_source_value IS NOT NULL AND sleep_end_at_utc IS NULL,
                ['invalid_sleep_end_timestamp'],
                ARRAY<STRING>[]
            ),
            IF(sleep_end_at_utc < sleep_start_at_utc, ['invalid_sleep_window'], ARRAY<STRING>[])
        ) AS _invalid_issues,
        ARRAY_CONCAT(
            IF(total_sleep_s IS NULL, ['missing_total_sleep_s'], ARRAY<STRING>[]),
            IF(sleep_score IS NULL, ['missing_sleep_score'], ARRAY<STRING>[])
        ) AS _missing_issues
    FROM normalized
)

SELECT
    health_date,
    source_user_id,
    identity_scope,
    source_system,
    source_table,
    source_record_key,
    sleep_calendar_date,
    score_qualifier,
    quality_key,
    score_feedback,
    hrv_status,
    body_battery_change,
    sleep_start_at_utc,
    sleep_end_at_utc,
    sleep_start_at_local,
    sleep_end_at_local,
    _ingested_at,
    IF(sleep_score BETWEEN 0 AND 100, sleep_score, NULL) AS sleep_score,
    IF(total_sleep_s >= 0, total_sleep_s, NULL) AS total_sleep_s,
    IF(deep_sleep_s >= 0, deep_sleep_s, NULL) AS deep_sleep_s,
    IF(light_sleep_s >= 0, light_sleep_s, NULL) AS light_sleep_s,
    IF(rem_sleep_s >= 0, rem_sleep_s, NULL) AS rem_sleep_s,
    IF(awake_sleep_s >= 0, awake_sleep_s, NULL) AS awake_sleep_s,
    IF(unmeasurable_sleep_s >= 0, unmeasurable_sleep_s, NULL) AS unmeasurable_sleep_s,
    IF(nap_s >= 0, nap_s, NULL) AS nap_s,
    IF(avg_overnight_hrv_ms > 0, avg_overnight_hrv_ms, NULL) AS avg_overnight_hrv_ms,
    IF(resting_hr_bpm > 0, resting_hr_bpm, NULL) AS resting_hr_bpm,
    IF(avg_spo2_pct > 0 AND avg_spo2_pct <= 100, avg_spo2_pct, NULL) AS avg_spo2_pct,
    IF(lowest_spo2_pct > 0 AND lowest_spo2_pct <= 100, lowest_spo2_pct, NULL) AS lowest_spo2_pct,
    IF(avg_respiration_rpm > 0, avg_respiration_rpm, NULL) AS avg_respiration_rpm,
    IF(lowest_respiration_rpm > 0, lowest_respiration_rpm, NULL) AS lowest_respiration_rpm,
    IF(highest_respiration_rpm > 0, highest_respiration_rpm, NULL) AS highest_respiration_rpm,
    IF(restless_moments >= 0, restless_moments, NULL) AS restless_moments,
    IF(body_battery_at_sleep BETWEEN 0 AND 100, body_battery_at_sleep, NULL) AS body_battery_at_sleep,
    IF(body_battery_at_wake BETWEEN 0 AND 100, body_battery_at_wake, NULL) AS body_battery_at_wake,
    STRUCT(
        sleep_calendar_date,
        sleep_score,
        score_qualifier,
        quality_key,
        score_feedback,
        total_sleep_s,
        deep_sleep_s,
        light_sleep_s,
        rem_sleep_s,
        awake_sleep_s,
        unmeasurable_sleep_s,
        nap_s,
        avg_overnight_hrv_ms,
        hrv_status,
        resting_hr_bpm,
        body_battery_change,
        avg_spo2_pct,
        lowest_spo2_pct,
        avg_respiration_rpm,
        lowest_respiration_rpm,
        highest_respiration_rpm,
        restless_moments,
        sleep_start_at_utc,
        sleep_end_at_utc,
        sleep_start_at_local,
        sleep_end_at_local,
        body_battery_at_sleep,
        body_battery_at_wake,
        sleep_start_source_value,
        sleep_end_source_value,
        sleep_start_local_source_value,
        sleep_end_local_source_value
    ) AS source_values,
    CASE
        WHEN ARRAY_LENGTH(_invalid_issues) > 0 THEN 'invalid' WHEN
            ARRAY_LENGTH(_missing_issues) > 0
            THEN 'partial'
        ELSE 'valid'
    END AS quality_status,
    ARRAY_CONCAT(_invalid_issues, _missing_issues) AS quality_issues
FROM qualified AS q
QUALIFY health_date IS NULL OR ROW_NUMBER() OVER (
    PARTITION BY source_user_id, health_date
    ORDER BY _ingested_at DESC, TO_JSON_STRING(q) ASC
) = 1
