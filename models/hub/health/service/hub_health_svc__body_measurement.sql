{{
    config(
        materialized='table',
        partition_by={'field': 'health_date', 'data_type': 'date', 'granularity': 'month'},
        cluster_by=['source_user_id']
    )
}}

-- Full current-state rebuild includes late corrections without a global watermark.
SELECT
    *,
    CURRENT_TIMESTAMP() AS _silver_processed_at
FROM {{ ref('hub_health_stg__body_measurement') }}
