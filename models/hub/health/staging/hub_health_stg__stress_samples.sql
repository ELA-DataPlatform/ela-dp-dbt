{{ config(materialized='view', tags=['health_timeseries']) }}

-- Internal point-grain preparation. Equal timestamps are retained via source offsets.
WITH raw_points AS (
    SELECT
        s.date AS health_date,
        CAST(s.userprofilepk AS STRING) AS source_user_id,
        'profile' AS identity_scope,
        'stress' AS source_table,
        TO_JSON_STRING(STRUCT(s.date AS source_date, CAST(s.userprofilepk AS STRING) AS source_user_id))
            AS source_record_key,
        'stress_values' AS source_array,
        sample_index,
        'stress_level' AS metric_name,
        'score' AS metric_unit,
        SAFE_CAST(v.stress_level AS FLOAT64) AS source_value,
        CAST(v.timestamp AS STRING) AS source_timestamp,
        CAST(NULL AS STRING) AS source_end_timestamp,
        CAST(NULL AS STRING) AS source_local_timestamp,
        'instant' AS timestamp_role,
        TO_JSON_STRING(v) AS source_sample,
        s._ingested_at,
        0.0 AS _min_value,
        100.0 AS _max_value,
        TRUE AS _min_inclusive
    FROM {{ ref('dlk_garmin_svc__stress') }} AS s,
        UNNEST(s.stress_values) AS v WITH OFFSET AS sample_index

    UNION ALL

    SELECT
        s.date AS health_date,
        CAST(s.userprofilepk AS STRING) AS source_user_id,
        'profile' AS identity_scope,
        'stress' AS source_table,
        TO_JSON_STRING(STRUCT(s.date AS source_date, CAST(s.userprofilepk AS STRING) AS source_user_id))
            AS source_record_key,
        'body_battery_values' AS source_array,
        sample_index,
        'body_battery_level' AS metric_name,
        'score' AS metric_unit,
        SAFE_CAST(v.body_battery_level AS FLOAT64) AS source_value,
        CAST(v.timestamp AS STRING) AS source_timestamp,
        CAST(NULL AS STRING) AS source_end_timestamp,
        CAST(NULL AS STRING) AS source_local_timestamp,
        'instant' AS timestamp_role,
        TO_JSON_STRING(v) AS source_sample,
        s._ingested_at,
        0.0 AS _min_value,
        100.0 AS _max_value,
        TRUE AS _min_inclusive
    FROM {{ ref('dlk_garmin_svc__stress') }} AS s,
        UNNEST(s.body_battery_values) AS v WITH OFFSET AS sample_index
),

{{ garmin_health_qualify_samples(garmin_health_timestamp) }}
