{{
    config(
        materialized='incremental',
        unique_key=['startTimestampGMT', 'activityType'],
        tags=['garmin']
    )
}}

SELECT *
FROM {{ ref('stg_garmin__all_day_events') }}

{%- if is_incremental() %}
    WHERE _ingested_at > (SELECT MAX(_ingested_at) FROM {{ this }})
{%- endif %}
