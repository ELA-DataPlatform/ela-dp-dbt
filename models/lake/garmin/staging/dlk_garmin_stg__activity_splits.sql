{{
    config(
        materialized='view',
        tags=['garmin'],
    )
}}

WITH source AS (
    SELECT
        * EXCEPT (split_summaries, typed_splits, splits),

        -- Parse split_summaries → nested splitSummaries array
        array(
            SELECT
                STRUCT(
                    cast(json_value(item, '$.splitType') AS STRING) AS split_type,
                    cast(json_value(item, '$.noOfSplits') AS INT64) AS no_of_splits,
                    cast(json_value(item, '$.distance') AS FLOAT64) AS distance,
                    cast(json_value(item, '$.duration') AS FLOAT64) AS duration,
                    cast(json_value(item, '$.movingDuration') AS FLOAT64) AS moving_duration,
                    cast(json_value(item, '$.elevationGain') AS FLOAT64) AS elevation_gain,
                    cast(json_value(item, '$.elevationLoss') AS FLOAT64) AS elevation_loss,
                    cast(json_value(item, '$.averageSpeed') AS FLOAT64) AS average_speed,
                    cast(json_value(item, '$.maxSpeed') AS FLOAT64) AS max_speed,
                    cast(json_value(item, '$.calories') AS FLOAT64) AS calories,
                    cast(json_value(item, '$.averageHR') AS FLOAT64) AS average_hr,
                    cast(json_value(item, '$.maxHR') AS FLOAT64) AS max_hr,
                    cast(json_value(item, '$.averageRunCadence') AS FLOAT64) AS average_run_cadence,
                    cast(json_value(item, '$.maxRunCadence') AS FLOAT64) AS max_run_cadence,
                    cast(json_value(item, '$.averagePower') AS FLOAT64) AS average_power,
                    cast(json_value(item, '$.maxPower') AS FLOAT64) AS max_power,
                    cast(json_value(item, '$.normalizedPower') AS FLOAT64) AS normalized_power,
                    cast(json_value(item, '$.avgGradeAdjustedSpeed') AS FLOAT64) AS avg_grade_adjusted_speed,
                    cast(json_value(item, '$.avgVerticalSpeed') AS FLOAT64) AS avg_vertical_speed,
                    cast(json_value(item, '$.avgStepFrequency') AS FLOAT64) AS avg_step_frequency,
                    cast(json_value(item, '$.maxElevationGain') AS FLOAT64) AS max_elevation_gain,
                    cast(json_value(item, '$.averageElevationGain') AS FLOAT64) AS average_elevation_gain,
                    cast(json_value(item, '$.maxDistance') AS FLOAT64) AS max_distance,
                    cast(json_value(item, '$.maxDistanceWithPrecision') AS FLOAT64) AS max_distance_with_precision,
                    cast(json_value(item, '$.averageTemperature') AS FLOAT64) AS average_temperature,
                    cast(json_value(item, '$.maxTemperature') AS FLOAT64) AS max_temperature,
                    cast(json_value(item, '$.minTemperature') AS FLOAT64) AS min_temperature
                )
            FROM unnest(json_query_array(split_summaries, '$.splitSummaries')) AS item
        ) AS split_summaries,

        -- Parse typed_splits → nested splits array
        array(
            SELECT
                STRUCT(
                    json_value(item, '$.startTimeGMT') AS start_time_gmt,
                    json_value(item, '$.startTimeLocal') AS start_time_local,
                    cast(json_value(item, '$.startLatitude') AS FLOAT64) AS start_latitude,
                    cast(json_value(item, '$.startLongitude') AS FLOAT64) AS start_longitude,
                    cast(json_value(item, '$.endLatitude') AS FLOAT64) AS end_latitude,
                    cast(json_value(item, '$.endLongitude') AS FLOAT64) AS end_longitude,
                    cast(json_value(item, '$.distance') AS FLOAT64) AS distance,
                    cast(json_value(item, '$.duration') AS FLOAT64) AS duration,
                    cast(json_value(item, '$.movingDuration') AS FLOAT64) AS moving_duration,
                    cast(json_value(item, '$.elapsedDuration') AS FLOAT64) AS elapsed_duration,
                    cast(json_value(item, '$.elevationGain') AS FLOAT64) AS elevation_gain,
                    cast(json_value(item, '$.elevationLoss') AS FLOAT64) AS elevation_loss,
                    cast(json_value(item, '$.averageSpeed') AS FLOAT64) AS average_speed,
                    cast(json_value(item, '$.maxSpeed') AS FLOAT64) AS max_speed,
                    cast(json_value(item, '$.calories') AS FLOAT64) AS calories,
                    cast(json_value(item, '$.averageHR') AS FLOAT64) AS average_hr,
                    cast(json_value(item, '$.maxHR') AS FLOAT64) AS max_hr,
                    cast(json_value(item, '$.averageRunCadence') AS FLOAT64) AS average_run_cadence,
                    cast(json_value(item, '$.maxRunCadence') AS FLOAT64) AS max_run_cadence,
                    cast(json_value(item, '$.averagePower') AS FLOAT64) AS average_power,
                    cast(json_value(item, '$.maxPower') AS FLOAT64) AS max_power,
                    cast(json_value(item, '$.normalizedPower') AS FLOAT64) AS normalized_power,
                    json_value(item, '$.intensityType') AS intensity_type,
                    cast(json_value(item, '$.messageIndex') AS INT64) AS message_index,
                    cast(json_value(item, '$.avgGradeAdjustedSpeed') AS FLOAT64) AS avg_grade_adjusted_speed,
                    cast(json_value(item, '$.averageTemperature') AS FLOAT64) AS average_temperature,
                    cast(json_value(item, '$.maxTemperature') AS FLOAT64) AS max_temperature,
                    cast(json_value(item, '$.minTemperature') AS FLOAT64) AS min_temperature
                )
            FROM unnest(json_query_array(typed_splits, '$.splits')) AS item
        ) AS typed_splits,

        -- Parse splits → nested lapDTOs array
        array(
            SELECT
                STRUCT(
                    json_value(item, '$.startTimeGMT') AS start_time_gmt,
                    cast(json_value(item, '$.startLatitude') AS FLOAT64) AS start_latitude,
                    cast(json_value(item, '$.startLongitude') AS FLOAT64) AS start_longitude,
                    cast(json_value(item, '$.endLatitude') AS FLOAT64) AS end_latitude,
                    cast(json_value(item, '$.endLongitude') AS FLOAT64) AS end_longitude,
                    cast(json_value(item, '$.distance') AS FLOAT64) AS distance,
                    cast(json_value(item, '$.duration') AS FLOAT64) AS duration,
                    cast(json_value(item, '$.movingDuration') AS FLOAT64) AS moving_duration,
                    cast(json_value(item, '$.elapsedDuration') AS FLOAT64) AS elapsed_duration,
                    cast(json_value(item, '$.elevationGain') AS FLOAT64) AS elevation_gain,
                    cast(json_value(item, '$.elevationLoss') AS FLOAT64) AS elevation_loss,
                    cast(json_value(item, '$.maxElevation') AS FLOAT64) AS max_elevation,
                    cast(json_value(item, '$.minElevation') AS FLOAT64) AS min_elevation,
                    cast(json_value(item, '$.averageSpeed') AS FLOAT64) AS average_speed,
                    cast(json_value(item, '$.averageMovingSpeed') AS FLOAT64) AS average_moving_speed,
                    cast(json_value(item, '$.maxSpeed') AS FLOAT64) AS max_speed,
                    cast(json_value(item, '$.calories') AS FLOAT64) AS calories,
                    cast(json_value(item, '$.bmrCalories') AS FLOAT64) AS bmr_calories,
                    cast(json_value(item, '$.averageHR') AS FLOAT64) AS average_hr,
                    cast(json_value(item, '$.maxHR') AS FLOAT64) AS max_hr,
                    cast(json_value(item, '$.averageRunCadence') AS FLOAT64) AS average_run_cadence,
                    cast(json_value(item, '$.maxRunCadence') AS FLOAT64) AS max_run_cadence,
                    cast(json_value(item, '$.averagePower') AS FLOAT64) AS average_power,
                    cast(json_value(item, '$.maxPower') AS FLOAT64) AS max_power,
                    cast(json_value(item, '$.minPower') AS FLOAT64) AS min_power,
                    cast(json_value(item, '$.normalizedPower') AS FLOAT64) AS normalized_power,
                    cast(json_value(item, '$.totalWork') AS FLOAT64) AS total_work,
                    cast(json_value(item, '$.groundContactTime') AS FLOAT64) AS ground_contact_time,
                    cast(json_value(item, '$.strideLength') AS FLOAT64) AS stride_length,
                    cast(json_value(item, '$.verticalOscillation') AS FLOAT64) AS vertical_oscillation,
                    cast(json_value(item, '$.verticalRatio') AS FLOAT64) AS vertical_ratio,
                    cast(json_value(item, '$.avgGradeAdjustedSpeed') AS FLOAT64) AS avg_grade_adjusted_speed,
                    cast(json_value(item, '$.averageTemperature') AS FLOAT64) AS average_temperature,
                    cast(json_value(item, '$.maxTemperature') AS FLOAT64) AS max_temperature,
                    cast(json_value(item, '$.minTemperature') AS FLOAT64) AS min_temperature,
                    json_value(item, '$.intensityType') AS intensity_type,
                    cast(json_value(item, '$.lapIndex') AS INT64) AS lap_index,
                    cast(json_value(item, '$.messageIndex') AS INT64) AS message_index,
                    cast(json_value(item, '$.maxVerticalSpeed') AS FLOAT64) AS max_vertical_speed,
                    cast(json_value(item, '$.directWorkoutComplianceScore') AS INT64) AS direct_workout_compliance_score
                )
            -- lapDTOs is a top-level column (direct JSON array), not nested under splits.
            -- Fallback to splits.lapDTOs for legacy records where lapDTOs column is null.
            FROM unnest(
                coalesce(
                    json_query_array(lapdtos),
                    json_query_array(splits, '$.lapDTOs')
                )
            ) AS item
        ) AS laps

    FROM {{ source('garmin', 'normalized_activity_splits') }}
),

deduplicated AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY activityid
            ORDER BY _ingested_at DESC
        ) AS _row_number
    FROM source
)

SELECT * EXCEPT (_row_number)
FROM deduplicated
WHERE _row_number = 1
