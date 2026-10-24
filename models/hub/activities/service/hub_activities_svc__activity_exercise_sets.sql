{{ config(materialized='table', cluster_by=['activity_id']) }}

-- Rebuild all canonical rows, including late details and historical corrections.
SELECT
    s.*,
    CURRENT_TIMESTAMP() AS _silver_processed_at
FROM {{ ref('hub_activities_stg__activity_exercise_sets') }} AS s
