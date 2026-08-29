{{
    config(
        materialized='incremental',
        unique_key=['userProfilePk', 'hrv_date'],
        tags=['garmin'],
        partition_by={
            'field': '_ingested_at',
            'data_type': 'timestamp',
            'granularity': 'month'
        }
    )
}}

SELECT *
FROM {{ ref('dlk_garmin_stg__hrv') }}

{% if is_incremental() %}
    WHERE _ingested_at > (SELECT max(_ingested_at) FROM {{ this }})
{% endif %}
