{{
    config(
        materialized='table',
        partition_by={'field': 'health_date', 'data_type': 'date', 'granularity': 'month'},
        cluster_by=['source_user_id'],
        tags=['health_timeseries']
    )
}}

-- One daily record with a typed repeated field; no second materialized point table.
WITH samples AS (
    SELECT
        health_date,
        source_user_id,
        identity_scope,
        ARRAY_AGG(
            STRUCT(
                sample_key,
                sample_index,
                source_array,
                metric_name,
                metric_unit,
                observed_at_utc,
                observed_at_local,
                end_at_utc,
                timestamp_role,
                measurement_value,
                source_value,
                source_timestamp,
                source_end_timestamp,
                source_local_timestamp,
                source_sample,
                quality_status,
                quality_issues
            )
            ORDER BY
                observed_at_utc IS NULL ASC,
                observed_at_utc ASC,
                metric_name ASC,
                source_array ASC,
                sample_index ASC
        ) AS timeseries
    FROM {{ ref('hub_health_stg__stress_samples') }}
    GROUP BY health_date, source_user_id, identity_scope
)

SELECT
    daily.*,
    COALESCE(samples.timeseries, []) AS timeseries,
    CURRENT_TIMESTAMP() AS _silver_processed_at
FROM {{ ref('hub_health_stg__stress_daily') }} AS daily
LEFT JOIN samples
    ON
        daily.health_date IS NOT DISTINCT FROM samples.health_date
        AND daily.source_user_id IS NOT DISTINCT FROM samples.source_user_id
        AND daily.identity_scope = samples.identity_scope
