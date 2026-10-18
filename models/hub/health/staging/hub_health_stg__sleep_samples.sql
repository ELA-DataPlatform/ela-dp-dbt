{{ config(materialized='view', tags=['health_timeseries']) }}

-- Internal point-grain preparation. Equal timestamps are retained via source offsets.
WITH raw_points AS (
    SELECT
        s.date AS health_date,
        CAST(NULL AS STRING) AS source_user_id,
        'account' AS identity_scope,
        'sleep' AS source_table,
        TO_JSON_STRING(STRUCT(s.date AS source_date, CAST(NULL AS STRING) AS source_user_id)) AS source_record_key,
        'sleep_levels' AS source_array,
        sample_index,
        'sleep_activity_level' AS metric_name,
        'garmin_activity_level' AS metric_unit,
        SAFE_CAST(v.activity_level AS FLOAT64) AS source_value,
        CAST(NULL AS INT64) AS reading_confidence,
        CAST(v.start_gmt AS STRING) AS source_timestamp,
        CAST(v.end_gmt AS STRING) AS source_end_timestamp,
        CAST(NULL AS STRING) AS source_local_timestamp,
        'start' AS timestamp_role,
        TO_JSON_STRING(v) AS source_sample,
        s._ingested_at,
        0.0 AS _min_value,
        CAST(NULL AS FLOAT64) AS _max_value,
        TRUE AS _min_inclusive
    FROM {{ ref('dlk_garmin_svc__sleep') }} AS s,
        UNNEST(s.sleep_levels) AS v WITH OFFSET AS sample_index

    UNION ALL

    SELECT
        s.date AS health_date,
        CAST(NULL AS STRING) AS source_user_id,
        'account' AS identity_scope,
        'sleep' AS source_table,
        TO_JSON_STRING(STRUCT(s.date AS source_date, CAST(NULL AS STRING) AS source_user_id)) AS source_record_key,
        'sleep_restless_moments' AS source_array,
        sample_index,
        'restless_moment' AS metric_name,
        'source_value' AS metric_unit,
        SAFE_CAST(v.value AS FLOAT64) AS source_value,
        CAST(NULL AS INT64) AS reading_confidence,
        CAST(v.start_gmt AS STRING) AS source_timestamp,
        CAST(NULL AS STRING) AS source_end_timestamp,
        CAST(NULL AS STRING) AS source_local_timestamp,
        'instant' AS timestamp_role,
        TO_JSON_STRING(v) AS source_sample,
        s._ingested_at,
        0.0 AS _min_value,
        CAST(NULL AS FLOAT64) AS _max_value,
        TRUE AS _min_inclusive
    FROM {{ ref('dlk_garmin_svc__sleep') }} AS s,
        UNNEST(s.sleep_restless_moments) AS v WITH OFFSET AS sample_index

    UNION ALL

    SELECT
        s.date AS health_date,
        CAST(NULL AS STRING) AS source_user_id,
        'account' AS identity_scope,
        'sleep' AS source_table,
        TO_JSON_STRING(STRUCT(s.date AS source_date, CAST(NULL AS STRING) AS source_user_id)) AS source_record_key,
        'sleep_heart_rate' AS source_array,
        sample_index,
        'heart_rate' AS metric_name,
        'bpm' AS metric_unit,
        SAFE_CAST(v.value AS FLOAT64) AS source_value,
        CAST(NULL AS INT64) AS reading_confidence,
        CAST(v.start_gmt AS STRING) AS source_timestamp,
        CAST(NULL AS STRING) AS source_end_timestamp,
        CAST(NULL AS STRING) AS source_local_timestamp,
        'instant' AS timestamp_role,
        TO_JSON_STRING(v) AS source_sample,
        s._ingested_at,
        0.0 AS _min_value,
        CAST(NULL AS FLOAT64) AS _max_value,
        FALSE AS _min_inclusive
    FROM {{ ref('dlk_garmin_svc__sleep') }} AS s,
        UNNEST(s.sleep_heart_rate) AS v WITH OFFSET AS sample_index

    UNION ALL

    SELECT
        s.date AS health_date,
        CAST(NULL AS STRING) AS source_user_id,
        'account' AS identity_scope,
        'sleep' AS source_table,
        TO_JSON_STRING(STRUCT(s.date AS source_date, CAST(NULL AS STRING) AS source_user_id)) AS source_record_key,
        'sleep_body_battery' AS source_array,
        sample_index,
        'body_battery_level' AS metric_name,
        'score' AS metric_unit,
        SAFE_CAST(v.value AS FLOAT64) AS source_value,
        CAST(NULL AS INT64) AS reading_confidence,
        CAST(v.start_gmt AS STRING) AS source_timestamp,
        CAST(NULL AS STRING) AS source_end_timestamp,
        CAST(NULL AS STRING) AS source_local_timestamp,
        'instant' AS timestamp_role,
        TO_JSON_STRING(v) AS source_sample,
        s._ingested_at,
        0.0 AS _min_value,
        100.0 AS _max_value,
        TRUE AS _min_inclusive
    FROM {{ ref('dlk_garmin_svc__sleep') }} AS s,
        UNNEST(s.sleep_body_battery) AS v WITH OFFSET AS sample_index

    UNION ALL

    SELECT
        s.date AS health_date,
        CAST(NULL AS STRING) AS source_user_id,
        'account' AS identity_scope,
        'sleep' AS source_table,
        TO_JSON_STRING(STRUCT(s.date AS source_date, CAST(NULL AS STRING) AS source_user_id)) AS source_record_key,
        'sleep_movement' AS source_array,
        sample_index,
        'movement_activity_level' AS metric_name,
        'garmin_activity_level' AS metric_unit,
        SAFE_CAST(v.activity_level AS FLOAT64) AS source_value,
        CAST(NULL AS INT64) AS reading_confidence,
        CAST(v.start_gmt AS STRING) AS source_timestamp,
        CAST(v.end_gmt AS STRING) AS source_end_timestamp,
        CAST(NULL AS STRING) AS source_local_timestamp,
        'start' AS timestamp_role,
        TO_JSON_STRING(v) AS source_sample,
        s._ingested_at,
        0.0 AS _min_value,
        CAST(NULL AS FLOAT64) AS _max_value,
        TRUE AS _min_inclusive
    FROM {{ ref('dlk_garmin_svc__sleep') }} AS s,
        UNNEST(s.sleep_movement) AS v WITH OFFSET AS sample_index

    UNION ALL

    SELECT
        s.date AS health_date,
        CAST(NULL AS STRING) AS source_user_id,
        'account' AS identity_scope,
        'sleep' AS source_table,
        TO_JSON_STRING(STRUCT(s.date AS source_date, CAST(NULL AS STRING) AS source_user_id)) AS source_record_key,
        'sleep_stress' AS source_array,
        sample_index,
        'stress_level' AS metric_name,
        'score' AS metric_unit,
        SAFE_CAST(v.value AS FLOAT64) AS source_value,
        CAST(NULL AS INT64) AS reading_confidence,
        CAST(v.start_gmt AS STRING) AS source_timestamp,
        CAST(NULL AS STRING) AS source_end_timestamp,
        CAST(NULL AS STRING) AS source_local_timestamp,
        'instant' AS timestamp_role,
        TO_JSON_STRING(v) AS source_sample,
        s._ingested_at,
        0.0 AS _min_value,
        100.0 AS _max_value,
        TRUE AS _min_inclusive
    FROM {{ ref('dlk_garmin_svc__sleep') }} AS s,
        UNNEST(s.sleep_stress) AS v WITH OFFSET AS sample_index

    UNION ALL

    SELECT
        s.date AS health_date,
        CAST(NULL AS STRING) AS source_user_id,
        'account' AS identity_scope,
        'sleep' AS source_table,
        TO_JSON_STRING(STRUCT(s.date AS source_date, CAST(NULL AS STRING) AS source_user_id)) AS source_record_key,
        'sleep_hrv' AS source_array,
        sample_index,
        'heart_rate_variability' AS metric_name,
        'ms' AS metric_unit,
        SAFE_CAST(v.value AS FLOAT64) AS source_value,
        CAST(NULL AS INT64) AS reading_confidence,
        CAST(v.start_gmt AS STRING) AS source_timestamp,
        CAST(NULL AS STRING) AS source_end_timestamp,
        CAST(NULL AS STRING) AS source_local_timestamp,
        'instant' AS timestamp_role,
        TO_JSON_STRING(v) AS source_sample,
        s._ingested_at,
        0.0 AS _min_value,
        CAST(NULL AS FLOAT64) AS _max_value,
        FALSE AS _min_inclusive
    FROM {{ ref('dlk_garmin_svc__sleep') }} AS s,
        UNNEST(s.sleep_hrv) AS v WITH OFFSET AS sample_index

    UNION ALL

    SELECT
        s.date AS health_date,
        CAST(NULL AS STRING) AS source_user_id,
        'account' AS identity_scope,
        'sleep' AS source_table,
        TO_JSON_STRING(STRUCT(s.date AS source_date, CAST(NULL AS STRING) AS source_user_id)) AS source_record_key,
        'sleep_breathing_disruption' AS source_array,
        sample_index,
        'breathing_disruption' AS metric_name,
        'source_value' AS metric_unit,
        SAFE_CAST(v.value AS FLOAT64) AS source_value,
        CAST(NULL AS INT64) AS reading_confidence,
        CAST(v.start_gmt AS STRING) AS source_timestamp,
        CAST(v.end_gmt AS STRING) AS source_end_timestamp,
        CAST(NULL AS STRING) AS source_local_timestamp,
        'start' AS timestamp_role,
        TO_JSON_STRING(v) AS source_sample,
        s._ingested_at,
        0.0 AS _min_value,
        CAST(NULL AS FLOAT64) AS _max_value,
        TRUE AS _min_inclusive
    FROM {{ ref('dlk_garmin_svc__sleep') }} AS s,
        UNNEST(s.sleep_breathing_disruption) AS v WITH OFFSET AS sample_index

    UNION ALL

    SELECT
        s.date AS health_date,
        CAST(NULL AS STRING) AS source_user_id,
        'account' AS identity_scope,
        'sleep' AS source_table,
        TO_JSON_STRING(STRUCT(s.date AS source_date, CAST(NULL AS STRING) AS source_user_id)) AS source_record_key,
        'sleep_respiration' AS source_array,
        sample_index,
        'respiration_rate' AS metric_name,
        'breaths_per_minute' AS metric_unit,
        SAFE_CAST(v.respiration_value AS FLOAT64) AS source_value,
        CAST(NULL AS INT64) AS reading_confidence,
        CAST(v.start_time_gmt AS STRING) AS source_timestamp,
        CAST(NULL AS STRING) AS source_end_timestamp,
        CAST(NULL AS STRING) AS source_local_timestamp,
        'instant' AS timestamp_role,
        TO_JSON_STRING(v) AS source_sample,
        s._ingested_at,
        0.0 AS _min_value,
        CAST(NULL AS FLOAT64) AS _max_value,
        FALSE AS _min_inclusive
    FROM {{ ref('dlk_garmin_svc__sleep') }} AS s,
        UNNEST(s.sleep_respiration) AS v WITH OFFSET AS sample_index

    UNION ALL

    SELECT
        s.date AS health_date,
        CAST(NULL AS STRING) AS source_user_id,
        'account' AS identity_scope,
        'sleep' AS source_table,
        TO_JSON_STRING(STRUCT(s.date AS source_date, CAST(NULL AS STRING) AS source_user_id)) AS source_record_key,
        'sleep_spo2' AS source_array,
        sample_index,
        'oxygen_saturation' AS metric_name,
        'percent' AS metric_unit,
        SAFE_CAST(v.spo2_reading AS FLOAT64) AS source_value,
        v.reading_confidence,
        CAST(v.epoch_timestamp AS STRING) AS source_timestamp,
        CAST(NULL AS STRING) AS source_end_timestamp,
        CAST(NULL AS STRING) AS source_local_timestamp,
        'instant' AS timestamp_role,
        TO_JSON_STRING(v) AS source_sample,
        s._ingested_at,
        0.0 AS _min_value,
        100.0 AS _max_value,
        FALSE AS _min_inclusive
    FROM {{ ref('dlk_garmin_svc__sleep') }} AS s,
        UNNEST(s.sleep_spo2) AS v WITH OFFSET AS sample_index

    UNION ALL

    SELECT
        s.date AS health_date,
        CAST(NULL AS STRING) AS source_user_id,
        'account' AS identity_scope,
        'sleep' AS source_table,
        TO_JSON_STRING(STRUCT(s.date AS source_date, CAST(NULL AS STRING) AS source_user_id)) AS source_record_key,
        'sleep_respiration_averages' AS source_array,
        sample_index,
        'respiration_average' AS metric_name,
        'breaths_per_minute' AS metric_unit,
        SAFE_CAST(v.respiration_average_value AS FLOAT64) AS source_value,
        CAST(NULL AS INT64) AS reading_confidence,
        CAST(v.epoch_end_timestamp_gmt AS STRING) AS source_timestamp,
        CAST(NULL AS STRING) AS source_end_timestamp,
        CAST(NULL AS STRING) AS source_local_timestamp,
        'end' AS timestamp_role,
        TO_JSON_STRING(v) AS source_sample,
        s._ingested_at,
        0.0 AS _min_value,
        CAST(NULL AS FLOAT64) AS _max_value,
        FALSE AS _min_inclusive
    FROM {{ ref('dlk_garmin_svc__sleep') }} AS s,
        UNNEST(s.sleep_respiration_averages) AS v WITH OFFSET AS sample_index

    UNION ALL

    SELECT
        s.date AS health_date,
        CAST(NULL AS STRING) AS source_user_id,
        'account' AS identity_scope,
        'sleep' AS source_table,
        TO_JSON_STRING(STRUCT(s.date AS source_date, CAST(NULL AS STRING) AS source_user_id)) AS source_record_key,
        'sleep_respiration_averages' AS source_array,
        sample_index,
        'respiration_low' AS metric_name,
        'breaths_per_minute' AS metric_unit,
        SAFE_CAST(v.respiration_low_value AS FLOAT64) AS source_value,
        CAST(NULL AS INT64) AS reading_confidence,
        CAST(v.epoch_end_timestamp_gmt AS STRING) AS source_timestamp,
        CAST(NULL AS STRING) AS source_end_timestamp,
        CAST(NULL AS STRING) AS source_local_timestamp,
        'end' AS timestamp_role,
        TO_JSON_STRING(v) AS source_sample,
        s._ingested_at,
        0.0 AS _min_value,
        CAST(NULL AS FLOAT64) AS _max_value,
        FALSE AS _min_inclusive
    FROM {{ ref('dlk_garmin_svc__sleep') }} AS s,
        UNNEST(s.sleep_respiration_averages) AS v WITH OFFSET AS sample_index

    UNION ALL

    SELECT
        s.date AS health_date,
        CAST(NULL AS STRING) AS source_user_id,
        'account' AS identity_scope,
        'sleep' AS source_table,
        TO_JSON_STRING(STRUCT(s.date AS source_date, CAST(NULL AS STRING) AS source_user_id)) AS source_record_key,
        'sleep_respiration_averages' AS source_array,
        sample_index,
        'respiration_high' AS metric_name,
        'breaths_per_minute' AS metric_unit,
        SAFE_CAST(v.respiration_high_value AS FLOAT64) AS source_value,
        CAST(NULL AS INT64) AS reading_confidence,
        CAST(v.epoch_end_timestamp_gmt AS STRING) AS source_timestamp,
        CAST(NULL AS STRING) AS source_end_timestamp,
        CAST(NULL AS STRING) AS source_local_timestamp,
        'end' AS timestamp_role,
        TO_JSON_STRING(v) AS source_sample,
        s._ingested_at,
        0.0 AS _min_value,
        CAST(NULL AS FLOAT64) AS _max_value,
        FALSE AS _min_inclusive
    FROM {{ ref('dlk_garmin_svc__sleep') }} AS s,
        UNNEST(s.sleep_respiration_averages) AS v WITH OFFSET AS sample_index
),

{{ garmin_health_qualify_samples(garmin_health_timestamp, include_reading_confidence=true) }}
