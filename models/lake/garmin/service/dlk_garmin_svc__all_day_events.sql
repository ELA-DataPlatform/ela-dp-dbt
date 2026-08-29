{{
    config(
        materialized='incremental',
        unique_key=['deviceId', 'date', 'startTimestampGMT'],
        tags=['garmin'],
        partition_by={
            'field': '_ingested_at',
            'data_type': 'timestamp',
            'granularity': 'month'
        }
    )
}}

SELECT *
FROM {{ ref('dlk_garmin_stg__all_day_events') }}

{% if is_incremental() %}
    WHERE _ingested_at > (SELECT max(_ingested_at) FROM {{ this }})
{% endif %}
