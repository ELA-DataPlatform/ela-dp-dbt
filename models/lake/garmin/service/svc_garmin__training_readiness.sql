{{
    config(
        materialized='incremental',
        unique_key='calendarDate',
        tags=['garmin']
    )
}}

SELECT *
FROM {{ ref('stg_garmin__training_readiness') }}

{%- if is_incremental() %}
    WHERE _ingested_at > (SELECT MAX(_ingested_at) FROM {{ this }})
{%- endif %}
