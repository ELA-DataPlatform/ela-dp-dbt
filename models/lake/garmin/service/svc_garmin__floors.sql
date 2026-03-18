{{
    config(
        materialized='incremental',
        unique_key='date',
        tags=['garmin']
    )
}}

SELECT *
FROM {{ ref('stg_garmin__floors') }}

{%- if is_incremental() %}
    WHERE _ingested_at > (SELECT MAX(_ingested_at) FROM {{ this }})
{%- endif %}
