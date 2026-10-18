{{
    config(
        materialized='table',
        partition_by={
            'field': 'activity_date',
            'data_type': 'date',
            'granularity': 'month'
        },
        cluster_by=['activity_type'],
        tags=['garmin']
    )
}}

-- Recompute all dependencies, including historical context changes and late details.
-- A table rebuild also migrates nested field types and refreshes the GPS route.
SELECT *
FROM {{ ref('hub_activities_stg__master_running_activities') }}
