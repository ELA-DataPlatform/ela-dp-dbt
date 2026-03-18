{{
    config(
        materialized='incremental',
        unique_key=['date', 'startGMT'],
        tags=['garmin']
    )
}}

SELECT *
FROM {{ ref('stg_garmin__steps') }}

{%- if is_incremental() %}
    WHERE _ingested_at > (SELECT MAX(_ingested_at) FROM {{ this }})
{%- endif %}
