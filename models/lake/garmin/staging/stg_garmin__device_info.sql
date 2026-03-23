
{{
    config(
        materialized='view',
        tags=['garmin'],
    )
}}

with source as (
    select * from {{ source('garmin', 'normalized_device_info') }}
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by deviceId
            order by _ingested_at desc
        ) as _row_number
    from source
)

select * except(_row_number)
from deduplicated
where _row_number = 1
