
{{
    config(
        materialized='view',
        tags=['garmin'],
    )
}}

with source as (
    select
        * except(hr_zones_data),

        -- Parse hr_zones_data → ARRAY<STRUCT>
        array(
            select struct(
                cast(json_value(item, '$.zoneNumber') as int64) as zone_number,
                cast(json_value(item, '$.secsInZone') as float64) as secs_in_zone,
                cast(json_value(item, '$.zoneLowBoundary') as int64) as zone_low_boundary
            )
            from unnest(json_query_array(hr_zones_data)) as item
        ) as hr_zones

    from {{ source('garmin', 'normalized_activity_hr_zones') }}
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
