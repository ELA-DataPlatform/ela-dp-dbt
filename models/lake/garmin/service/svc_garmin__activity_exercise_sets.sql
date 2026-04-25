{{
    config(
        materialized='incremental',
        unique_key='activityId',
        tags=['garmin'],
        partition_by={
            'field': '_ingested_at',
            'data_type': 'timestamp',
            'granularity': 'month'
        }
    )
}}

SELECT *
FROM {{ ref('stg_garmin__activity_exercise_sets') }}

{% if is_incremental() %}
    WHERE _ingested_at > (SELECT max(_ingested_at) FROM {{ this }})
{% endif %}
