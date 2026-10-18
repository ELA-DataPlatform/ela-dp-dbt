{{ config(materialized='view', tags=['health_timeseries']) }}

-- Internal point-grain preparation. Equal timestamps are retained via source offsets.
WITH raw_points AS (
    SELECT
        s.hrv_date AS health_date,
        'profile' AS identity_scope,
        'hrv' AS source_table,
        'hrv_readings' AS source_array,
        sample_index,
        'heart_rate_variability' AS metric_name,
        'ms' AS metric_unit,
        'instant' AS timestamp_role,
        s._ingested_at,
        0.0 AS _min_value,
        FALSE AS _min_inclusive,
        CAST(s.userprofilepk AS STRING) AS source_user_id,
        TO_JSON_STRING(STRUCT(s.hrv_date AS source_date, CAST(s.userprofilepk AS STRING) AS source_user_id))
            AS source_record_key,
        SAFE_CAST(v.hrv_value AS FLOAT64) AS source_value,
        CAST(v.reading_time_gmt AS STRING) AS source_timestamp,
        CAST(NULL AS STRING) AS source_end_timestamp,
        CAST(v.reading_time_local AS STRING) AS source_local_timestamp,
        TO_JSON_STRING(v) AS source_sample,
        CAST(NULL AS FLOAT64) AS _max_value
    FROM {{ ref('dlk_garmin_svc__hrv') }} AS s,
        UNNEST(s.hrv_readings) AS v WITH OFFSET AS sample_index
),

{{ garmin_health_qualify_samples(garmin_health_timestamp) }}
