
{{
    config(
        materialized='view',
        tags=['garmin'],
    )
}}

with source as (
    select
        * except(stressValuesArray, stressValueDescriptorsDTOList,
                 bodyBatteryValuesArray, bodyBatteryValueDescriptorsDTOList),

        -- Parse stressValuesArray → ARRAY<STRUCT>
        array(
            select struct(
                cast(json_value(item, '$.timestamp') as int64) as timestamp,
                cast(json_value(item, '$.type') as int64) as stress_level
            )
            from unnest(json_query_array(stressValuesArray)) as item
        ) as stress_values,

        -- Parse bodyBatteryValuesArray → ARRAY<STRUCT>
        array(
            select struct(
                cast(json_value(item, '$.timestamp') as int64) as timestamp,
                json_value(item, '$.type') as type,
                cast(json_value(item, '$.value') as int64) as value,
                cast(json_value(item, '$.score') as float64) as score
            )
            from unnest(json_query_array(bodyBatteryValuesArray)) as item
        ) as body_battery_values

    from {{ source('garmin', 'normalized_stress') }}
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by userProfilePK, date
            order by _ingested_at desc
        ) as _row_number
    from source
)

select * except(_row_number)
from deduplicated
where _row_number = 1
