
{{
    config(
        materialized='view',
        tags=['garmin'],
    )
}}

with source as (
    select
        * except(split_summaries, typed_splits, splits),

        -- Parse split_summaries → nested splitSummaries array
        array(
            select struct(
                cast(json_value(item, '$.splitType') as string) as split_type,
                cast(json_value(item, '$.noOfSplits') as int64) as no_of_splits,
                cast(json_value(item, '$.distance') as float64) as distance,
                cast(json_value(item, '$.duration') as float64) as duration,
                cast(json_value(item, '$.movingDuration') as float64) as moving_duration,
                cast(json_value(item, '$.elevationGain') as float64) as elevation_gain,
                cast(json_value(item, '$.elevationLoss') as float64) as elevation_loss,
                cast(json_value(item, '$.averageSpeed') as float64) as average_speed,
                cast(json_value(item, '$.maxSpeed') as float64) as max_speed,
                cast(json_value(item, '$.calories') as float64) as calories,
                cast(json_value(item, '$.averageHR') as float64) as average_hr,
                cast(json_value(item, '$.maxHR') as float64) as max_hr,
                cast(json_value(item, '$.averageRunCadence') as float64) as average_run_cadence,
                cast(json_value(item, '$.maxRunCadence') as float64) as max_run_cadence,
                cast(json_value(item, '$.averagePower') as float64) as average_power,
                cast(json_value(item, '$.maxPower') as float64) as max_power,
                cast(json_value(item, '$.normalizedPower') as float64) as normalized_power,
                cast(json_value(item, '$.avgGradeAdjustedSpeed') as float64) as avg_grade_adjusted_speed,
                cast(json_value(item, '$.avgVerticalSpeed') as float64) as avg_vertical_speed,
                cast(json_value(item, '$.avgStepFrequency') as float64) as avg_step_frequency,
                cast(json_value(item, '$.maxElevationGain') as float64) as max_elevation_gain,
                cast(json_value(item, '$.averageElevationGain') as float64) as average_elevation_gain,
                cast(json_value(item, '$.maxDistance') as float64) as max_distance,
                cast(json_value(item, '$.maxDistanceWithPrecision') as float64) as max_distance_with_precision,
                cast(json_value(item, '$.averageTemperature') as float64) as average_temperature,
                cast(json_value(item, '$.maxTemperature') as float64) as max_temperature,
                cast(json_value(item, '$.minTemperature') as float64) as min_temperature
            )
            from unnest(json_query_array(split_summaries, '$.splitSummaries')) as item
        ) as split_summaries,

        -- Parse typed_splits → nested splits array
        array(
            select struct(
                json_value(item, '$.startTimeGMT') as start_time_gmt,
                json_value(item, '$.startTimeLocal') as start_time_local,
                cast(json_value(item, '$.startLatitude') as float64) as start_latitude,
                cast(json_value(item, '$.startLongitude') as float64) as start_longitude,
                cast(json_value(item, '$.endLatitude') as float64) as end_latitude,
                cast(json_value(item, '$.endLongitude') as float64) as end_longitude,
                cast(json_value(item, '$.distance') as float64) as distance,
                cast(json_value(item, '$.duration') as float64) as duration,
                cast(json_value(item, '$.movingDuration') as float64) as moving_duration,
                cast(json_value(item, '$.elapsedDuration') as float64) as elapsed_duration,
                cast(json_value(item, '$.elevationGain') as float64) as elevation_gain,
                cast(json_value(item, '$.elevationLoss') as float64) as elevation_loss,
                cast(json_value(item, '$.averageSpeed') as float64) as average_speed,
                cast(json_value(item, '$.maxSpeed') as float64) as max_speed,
                cast(json_value(item, '$.calories') as float64) as calories,
                cast(json_value(item, '$.averageHR') as float64) as average_hr,
                cast(json_value(item, '$.maxHR') as float64) as max_hr,
                cast(json_value(item, '$.averageRunCadence') as float64) as average_run_cadence,
                cast(json_value(item, '$.maxRunCadence') as float64) as max_run_cadence,
                cast(json_value(item, '$.averagePower') as float64) as average_power,
                cast(json_value(item, '$.maxPower') as float64) as max_power,
                cast(json_value(item, '$.normalizedPower') as float64) as normalized_power,
                json_value(item, '$.intensityType') as intensity_type,
                cast(json_value(item, '$.messageIndex') as int64) as message_index,
                cast(json_value(item, '$.avgGradeAdjustedSpeed') as float64) as avg_grade_adjusted_speed,
                cast(json_value(item, '$.averageTemperature') as float64) as average_temperature,
                cast(json_value(item, '$.maxTemperature') as float64) as max_temperature,
                cast(json_value(item, '$.minTemperature') as float64) as min_temperature
            )
            from unnest(json_query_array(typed_splits, '$.splits')) as item
        ) as typed_splits,

        -- Parse splits → nested lapDTOs array
        array(
            select struct(
                json_value(item, '$.startTimeGMT') as start_time_gmt,
                cast(json_value(item, '$.startLatitude') as float64) as start_latitude,
                cast(json_value(item, '$.startLongitude') as float64) as start_longitude,
                cast(json_value(item, '$.endLatitude') as float64) as end_latitude,
                cast(json_value(item, '$.endLongitude') as float64) as end_longitude,
                cast(json_value(item, '$.distance') as float64) as distance,
                cast(json_value(item, '$.duration') as float64) as duration,
                cast(json_value(item, '$.movingDuration') as float64) as moving_duration,
                cast(json_value(item, '$.elapsedDuration') as float64) as elapsed_duration,
                cast(json_value(item, '$.elevationGain') as float64) as elevation_gain,
                cast(json_value(item, '$.elevationLoss') as float64) as elevation_loss,
                cast(json_value(item, '$.maxElevation') as float64) as max_elevation,
                cast(json_value(item, '$.minElevation') as float64) as min_elevation,
                cast(json_value(item, '$.averageSpeed') as float64) as average_speed,
                cast(json_value(item, '$.averageMovingSpeed') as float64) as average_moving_speed,
                cast(json_value(item, '$.maxSpeed') as float64) as max_speed,
                cast(json_value(item, '$.calories') as float64) as calories,
                cast(json_value(item, '$.bmrCalories') as float64) as bmr_calories,
                cast(json_value(item, '$.averageHR') as float64) as average_hr,
                cast(json_value(item, '$.maxHR') as float64) as max_hr,
                cast(json_value(item, '$.averageRunCadence') as float64) as average_run_cadence,
                cast(json_value(item, '$.maxRunCadence') as float64) as max_run_cadence,
                cast(json_value(item, '$.averagePower') as float64) as average_power,
                cast(json_value(item, '$.maxPower') as float64) as max_power,
                cast(json_value(item, '$.minPower') as float64) as min_power,
                cast(json_value(item, '$.normalizedPower') as float64) as normalized_power,
                cast(json_value(item, '$.totalWork') as float64) as total_work,
                cast(json_value(item, '$.groundContactTime') as float64) as ground_contact_time,
                cast(json_value(item, '$.strideLength') as float64) as stride_length,
                cast(json_value(item, '$.verticalOscillation') as float64) as vertical_oscillation,
                cast(json_value(item, '$.verticalRatio') as float64) as vertical_ratio,
                cast(json_value(item, '$.avgGradeAdjustedSpeed') as float64) as avg_grade_adjusted_speed,
                cast(json_value(item, '$.averageTemperature') as float64) as average_temperature,
                cast(json_value(item, '$.maxTemperature') as float64) as max_temperature,
                cast(json_value(item, '$.minTemperature') as float64) as min_temperature,
                json_value(item, '$.intensityType') as intensity_type,
                cast(json_value(item, '$.lapIndex') as int64) as lap_index,
                cast(json_value(item, '$.messageIndex') as int64) as message_index,
                cast(json_value(item, '$.maxVerticalSpeed') as float64) as max_vertical_speed,
                cast(json_value(item, '$.directWorkoutComplianceScore') as int64) as direct_workout_compliance_score
            )
            from unnest(json_query_array(splits, '$.lapDTOs')) as item
        ) as laps

    from {{ source('garmin', 'normalized_activity_splits') }}
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
