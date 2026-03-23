
{{
    config(
        materialized='view',
        tags=['garmin'],
    )
}}

with source as (
    select
        * except(floorValuesArray, floorsValueDescriptorDTOList),

        -- Parse floorValuesArray → ARRAY<STRUCT>
        array(
            select struct(
                json_value(item, '$.start_time') as start_time,
                json_value(item, '$.end_time') as end_time,
                cast(json_value(item, '$.ascended') as int64) as ascended,
                cast(json_value(item, '$.descended') as int64) as descended
            )
            from unnest(json_query_array(floorValuesArray)) as item
        ) as floor_values

    from {{ source('garmin', 'normalized_floors') }}
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by date
            order by _ingested_at desc
        ) as _row_number
    from source
)

select * except(_row_number)
from deduplicated
where _row_number = 1
