{{
    config(
        materialized='incremental',
        unique_key=['userProfileId', 'date'],
        merge_update_columns=[
            'statisticsStartDate',
            'statisticsEndDate',
            'data_type',
            'resting_heart_rate',
            'rhr_calendar_date',
            '_ingested_at',
        ],
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
FROM {{ ref('dlk_garmin_stg__rhr_daily') }}
