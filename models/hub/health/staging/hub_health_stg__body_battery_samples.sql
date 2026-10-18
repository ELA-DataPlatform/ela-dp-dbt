{{ config(materialized='view', tags=['health_timeseries']) }}

-- Internal point-grain preparation. Equal timestamps are retained via source offsets.
WITH raw_points AS (
    SELECT
        s.date AS health_date,
        CAST(NULL AS STRING) AS source_user_id,
        'account' AS identity_scope,
        'body_battery' AS source_table,
        TO_JSON_STRING(STRUCT(s.date AS source_date, CAST(NULL AS STRING) AS source_user_id)) AS source_record_key,
        'body_battery_timeseries' AS source_array,
        sample_index,
        'body_battery_level' AS metric_name,
        'score' AS metric_unit,
        SAFE_CAST(v.value AS FLOAT64) AS source_value,
        CAST(v.timestamp_gmt AS STRING) AS source_timestamp,
        CAST(NULL AS STRING) AS source_end_timestamp,
        CAST(NULL AS STRING) AS source_local_timestamp,
        'instant' AS timestamp_role,
        TO_JSON_STRING(v) AS source_sample,
        s._ingested_at,
        0.0 AS _min_value,
        100.0 AS _max_value,
        TRUE AS _min_inclusive
    FROM {{ ref('dlk_garmin_svc__body_battery') }} AS s,
        UNNEST(s.body_battery_timeseries) AS v WITH OFFSET AS sample_index

    UNION ALL

    SELECT
        s.date AS health_date,
        CAST(NULL AS STRING) AS source_user_id,
        'account' AS identity_scope,
        'body_battery' AS source_table,
        TO_JSON_STRING(STRUCT(s.date AS source_date, CAST(NULL AS STRING) AS source_user_id)) AS source_record_key,
        'body_battery_values' AS source_array,
        sample_index,
        'body_battery_level' AS metric_name,
        'score' AS metric_unit,
        SAFE_CAST(v.value AS FLOAT64) AS source_value,
        CAST(v.timestamp AS STRING) AS source_timestamp,
        CAST(NULL AS STRING) AS source_end_timestamp,
        CAST(NULL AS STRING) AS source_local_timestamp,
        'instant' AS timestamp_role,
        TO_JSON_STRING(v) AS source_sample,
        s._ingested_at,
        0.0 AS _min_value,
        100.0 AS _max_value,
        TRUE AS _min_inclusive
    FROM {{ ref('dlk_garmin_svc__body_battery') }} AS s,
        UNNEST(s.body_battery_values) AS v WITH OFFSET AS sample_index
    WHERE ARRAY_LENGTH(s.body_battery_timeseries) = 0

    UNION ALL

    SELECT
        s.date AS health_date,
        CAST(NULL AS STRING) AS source_user_id,
        'account' AS identity_scope,
        'body_battery' AS source_table,
        TO_JSON_STRING(STRUCT(s.date AS source_date, CAST(NULL AS STRING) AS source_user_id)) AS source_record_key,
        'stress_timeseries' AS source_array,
        sample_index,
        'stress_level' AS metric_name,
        'score' AS metric_unit,
        SAFE_CAST(v.stress_level AS FLOAT64) AS source_value,
        CAST(v.timestamp_gmt AS STRING) AS source_timestamp,
        CAST(NULL AS STRING) AS source_end_timestamp,
        CAST(NULL AS STRING) AS source_local_timestamp,
        'instant' AS timestamp_role,
        TO_JSON_STRING(v) AS source_sample,
        s._ingested_at,
        0.0 AS _min_value,
        100.0 AS _max_value,
        TRUE AS _min_inclusive
    FROM {{ ref('dlk_garmin_svc__body_battery') }} AS s,
        UNNEST(s.stress_timeseries) AS v WITH OFFSET AS sample_index
),

{{ garmin_health_qualify_samples(garmin_health_timestamp) }}
