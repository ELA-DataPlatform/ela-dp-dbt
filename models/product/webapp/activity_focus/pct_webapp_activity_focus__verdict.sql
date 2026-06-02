{{
    config(
        materialized='table',
        tags=['garmin', 'webapp']
    )
}}

SELECT
    hub.activity_id,
    hub.activity_date,
    CAST(NULL AS STRING) AS tldr,
    CAST(NULL AS STRING) AS strengths_json,
    CAST(NULL AS STRING) AS warnings_json,
    CAST(NULL AS STRING) AS recommendations_json,
    CAST(NULL AS TIMESTAMP) AS generated_at
FROM {{ ref('svc_hub__master_running_activities') }} AS hub
