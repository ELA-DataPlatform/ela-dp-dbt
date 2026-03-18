{{
    config(
        materialized='incremental',
        unique_key=['startDate', 'endDate'],
        tags=['garmin']
    )
}}

SELECT *
FROM {{ ref('stg_garmin__endurance_score') }}

{%- if is_incremental() %}
    WHERE _ingested_at > (SELECT MAX(_ingested_at) FROM {{ this }})
{%- endif %}
