{{
    config(
        materialized='incremental',
        unique_key='deviceId',
        tags=['garmin']
    )
}}

SELECT *
FROM {{ ref('stg_garmin__device_info') }}

{%- if is_incremental() %}
    WHERE _ingested_at > (SELECT MAX(_ingested_at) FROM {{ this }})
{%- endif %}
