{{
    config(
        materialized='incremental',
        unique_key='activityId',
        tags=['garmin']
    )
}}

SELECT *
FROM {{ ref('stg_garmin__activity_exercise_sets') }}

{%- if is_incremental() %}
    WHERE _ingested_at > (SELECT MAX(_ingested_at) FROM {{ this }})
{%- endif %}
