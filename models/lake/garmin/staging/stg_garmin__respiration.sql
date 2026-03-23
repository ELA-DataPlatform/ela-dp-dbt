
{{
    config(
        materialized='view',
        tags=['garmin'],
    )
}}

with source as (
    select
        * except(respirationValuesArray, respirationAveragesValuesArray,
                 respirationValueDescriptorsDTOList, respirationAveragesValueDescriptorDTOList),

        -- Parse respirationValuesArray → ARRAY<STRUCT>
        array(
            select struct(
                cast(json_value(item, '$.timestamp') as int64) as timestamp,
                cast(json_value(item, '$.value') as float64) as value
            )
            from unnest(json_query_array(respirationValuesArray)) as item
        ) as respiration_values,

        -- Parse respirationAveragesValuesArray → ARRAY<STRUCT>
        array(
            select struct(
                cast(json_value(item, '$.timestamp') as int64) as timestamp,
                cast(json_value(item, '$.average') as float64) as average,
                cast(json_value(item, '$.high') as float64) as high,
                cast(json_value(item, '$.low') as float64) as low
            )
            from unnest(json_query_array(respirationAveragesValuesArray)) as item
        ) as respiration_averages

    from {{ source('garmin', 'normalized_respiration') }}
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
