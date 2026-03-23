
{{
    config(
        materialized='view',
        tags=['garmin'],
    )
}}

with source as (
    select
        * except(splitSummaries, summarizedExerciseSets, summarizedDiveInfo),

        -- Parse splitSummaries → ARRAY<STRUCT>
        array(
            select struct(
                json_value(item, '$.splitType') as split_type,
                cast(json_value(item, '$.noOfSplits') as int64) as no_of_splits,
                cast(json_value(item, '$.distance') as float64) as distance,
                cast(json_value(item, '$.duration') as float64) as duration,
                cast(json_value(item, '$.totalAscent') as float64) as total_ascent,
                cast(json_value(item, '$.elevationLoss') as float64) as elevation_loss,
                cast(json_value(item, '$.averageSpeed') as float64) as average_speed,
                cast(json_value(item, '$.maxSpeed') as float64) as max_speed,
                cast(json_value(item, '$.maxElevationGain') as float64) as max_elevation_gain,
                cast(json_value(item, '$.averageElevationGain') as float64) as average_elevation_gain,
                cast(json_value(item, '$.maxDistance') as float64) as max_distance,
                cast(json_value(item, '$.numClimbSends') as int64) as num_climb_sends,
                cast(json_value(item, '$.numFalls') as int64) as num_falls
            )
            from unnest(json_query_array(splitSummaries)) as item
        ) as split_summaries

    from {{ source('garmin', 'normalized_activities') }}
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
