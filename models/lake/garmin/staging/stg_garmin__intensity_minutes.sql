
{{
    config(
        materialized='view',
        tags=['garmin'],
    )
}}

with source as (
    select
        * except(imValuesArray, imValueDescriptorsDTOList),

        -- Parse imValuesArray → ARRAY<STRUCT>
        array(
            select struct(
                cast(json_value(item, '$.timestamp') as int64) as timestamp,
                cast(json_value(item, '$.value') as int64) as value
            )
            from unnest(json_query_array(imValuesArray)) as item
        ) as intensity_minutes_values

    from {{ source('garmin', 'normalized_intensity_minutes') }}
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
