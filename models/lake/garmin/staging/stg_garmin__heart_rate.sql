
{{
    config(
        materialized='view',
        tags=['garmin'],
    )
}}

with source as (
    select
        * except(heartRateValues, heartRateValueDescriptors, abnormalHRValuesArray),

        -- Parse heartRateValues JSON array → ARRAY<STRUCT>
        array(
            select struct(
                cast(json_value(item, '$.timestamp') as int64) as timestamp,
                cast(json_value(item, '$.value') as int64) as value
            )
            from unnest(json_query_array(heartRateValues)) as item
        ) as heart_rate_values,

        -- Parse abnormalHRValuesArray JSON array → ARRAY<STRUCT>
        array(
            select struct(
                cast(json_value(item, '$.timestamp') as int64) as timestamp,
                cast(json_value(item, '$.value') as int64) as value
            )
            from unnest(json_query_array(abnormalHRValuesArray)) as item
        ) as abnormal_hr_values

    from {{ source('garmin', 'normalized_heart_rate') }}
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
