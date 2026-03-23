
{{
    config(
        materialized='view',
        tags=['garmin'],
    )
}}

with source as (
    select * from {{ source('garmin', 'normalized_activity_exercise_sets') }}
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by activityId
            order by _ingested_at desc
        ) as _row_number
    from source
)

select * except(_row_number)
from deduplicated
where _row_number = 1
