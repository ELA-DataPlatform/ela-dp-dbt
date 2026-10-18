{{
    config(
        materialized='incremental',
        unique_key=['userProfilePK', 'startDate'],
        tags=['garmin'],
        partition_by={
            'field': '_ingested_at',
            'data_type': 'timestamp',
            'granularity': 'month'
        }
    )
}}

-- Reconcile every canonical staging row, including late records and corrections.
SELECT *
FROM {{ ref('dlk_garmin_stg__endurance_score') }}
