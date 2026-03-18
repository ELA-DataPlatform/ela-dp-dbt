{{
    config(
        materialized='incremental',
        unique_key='activityId',
        tags=['garmin']
    )
}}

SELECT *
FROM {{ ref('stg_garmin__activity_details') }}

{%- if is_incremental() %}
    WHERE _ingested_at > (SELECT MAX(_ingested_at) FROM {{ this }})
{%- endif %}
