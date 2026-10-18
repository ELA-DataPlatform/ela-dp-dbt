{{ config(materialized='view', tags=['health_timeseries']) }}

-- Internal point-grain preparation. Equal timestamps are retained via source offsets.
WITH raw_points AS (
    SELECT
        s.date AS health_date,
        CAST(s.userprofilepk AS STRING) AS source_user_id,
        'profile' AS identity_scope,
        'heart_rate' AS source_table,
        TO_JSON_STRING(STRUCT(s.date AS source_date, CAST(s.userprofilepk AS STRING) AS source_user_id))
            AS source_record_key,
        'heart_rate_values' AS source_array,
        sample_index,
        'heart_rate' AS metric_name,
        'bpm' AS metric_unit,
        SAFE_CAST(v.value AS FLOAT64) AS source_value,
        CAST(v.timestamp AS STRING) AS source_timestamp,
        CAST(NULL AS STRING) AS source_end_timestamp,
        CAST(NULL AS STRING) AS source_local_timestamp,
        'instant' AS timestamp_role,
        TO_JSON_STRING(v) AS source_sample,
        s._ingested_at,
        0.0 AS _min_value,
        CAST(NULL AS FLOAT64) AS _max_value,
        FALSE AS _min_inclusive
    FROM {{ ref('dlk_garmin_svc__heart_rate') }} AS s,
        UNNEST(s.heart_rate_values) AS v WITH OFFSET AS sample_index

    UNION ALL

    SELECT
        s.date AS health_date,
        CAST(s.userprofilepk AS STRING) AS source_user_id,
        'profile' AS identity_scope,
        'heart_rate' AS source_table,
        TO_JSON_STRING(STRUCT(s.date AS source_date, CAST(s.userprofilepk AS STRING) AS source_user_id))
            AS source_record_key,
        'abnormal_hr_values' AS source_array,
        sample_index,
        'abnormal_heart_rate' AS metric_name,
        'bpm' AS metric_unit,
        SAFE_CAST(v.value AS FLOAT64) AS source_value,
        CAST(v.timestamp AS STRING) AS source_timestamp,
        CAST(NULL AS STRING) AS source_end_timestamp,
        CAST(NULL AS STRING) AS source_local_timestamp,
        'instant' AS timestamp_role,
        TO_JSON_STRING(v) AS source_sample,
        s._ingested_at,
        0.0 AS _min_value,
        CAST(NULL AS FLOAT64) AS _max_value,
        FALSE AS _min_inclusive
    FROM {{ ref('dlk_garmin_svc__heart_rate') }} AS s,
        UNNEST(s.abnormal_hr_values) AS v WITH OFFSET AS sample_index
),

{{ garmin_health_qualify_samples(garmin_health_timestamp) }}
