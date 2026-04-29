{{
    config(
        materialized='view',
        tags=['garmin'],
    )
}}

WITH source AS (
    SELECT
        * EXCEPT (
            splitsummaries,
            summarizedexercisesets,
            summarizeddiveinfo,
            activitytypedto,
            eventtypedto,
            timezoneunitdto,
            accesscontrolruledto,
            summarydto,
            metadatadto
        ),

        -- Parse activityTypeDTO → STRUCT
        STRUCT(
            CAST(JSON_VALUE(activitytypedto, '$.typeId') AS INT64) AS type_id,
            JSON_VALUE(activitytypedto, '$.typeKey') AS type_key,
            CAST(JSON_VALUE(activitytypedto, '$.parentTypeId') AS INT64) AS parent_type_id,
            CAST(JSON_VALUE(activitytypedto, '$.isHidden') AS BOOL) AS is_hidden,
            CAST(JSON_VALUE(activitytypedto, '$.restricted') AS BOOL) AS restricted,
            CAST(JSON_VALUE(activitytypedto, '$.trimmable') AS BOOL) AS trimmable
        ) AS activity_type,

        -- Parse eventTypeDTO → STRUCT
        STRUCT(
            CAST(JSON_VALUE(eventtypedto, '$.typeId') AS INT64) AS type_id,
            JSON_VALUE(eventtypedto, '$.typeKey') AS type_key,
            CAST(JSON_VALUE(eventtypedto, '$.sortOrder') AS INT64) AS sort_order
        ) AS event_type,

        -- Parse timeZoneUnitDTO → STRUCT
        STRUCT(
            CAST(JSON_VALUE(timezoneunitdto, '$.unitId') AS INT64) AS unit_id,
            JSON_VALUE(timezoneunitdto, '$.unitKey') AS unit_key,
            JSON_VALUE(timezoneunitdto, '$.timeZone') AS time_zone
        ) AS time_zone_unit,

        -- Parse accessControlRuleDTO → STRUCT
        STRUCT(
            CAST(JSON_VALUE(accesscontrolruledto, '$.typeId') AS INT64) AS type_id,
            JSON_VALUE(accesscontrolruledto, '$.typeKey') AS type_key
        ) AS access_control_rule,

        -- Parse splitSummaries → ARRAY<STRUCT>
        ARRAY(
            SELECT
                STRUCT(
                    JSON_VALUE(item, '$.splitType') AS split_type,
                    CAST(JSON_VALUE(item, '$.noOfSplits') AS INT64) AS no_of_splits,
                    CAST(JSON_VALUE(item, '$.distance') AS FLOAT64) AS distance,
                    CAST(JSON_VALUE(item, '$.duration') AS FLOAT64) AS duration,
                    CAST(JSON_VALUE(item, '$.totalAscent') AS FLOAT64) AS total_ascent,
                    CAST(JSON_VALUE(item, '$.elevationLoss') AS FLOAT64) AS elevation_loss,
                    CAST(JSON_VALUE(item, '$.averageSpeed') AS FLOAT64) AS average_speed,
                    CAST(JSON_VALUE(item, '$.maxSpeed') AS FLOAT64) AS max_speed,
                    CAST(JSON_VALUE(item, '$.maxElevationGain') AS FLOAT64) AS max_elevation_gain,
                    CAST(JSON_VALUE(item, '$.averageElevationGain') AS FLOAT64) AS average_elevation_gain,
                    CAST(JSON_VALUE(item, '$.maxDistance') AS FLOAT64) AS max_distance,
                    CAST(JSON_VALUE(item, '$.numClimbSends') AS INT64) AS num_climb_sends,
                    CAST(JSON_VALUE(item, '$.numFalls') AS INT64) AS num_falls
                )
            FROM UNNEST(JSON_QUERY_ARRAY(splitsummaries)) AS item
        ) AS split_summaries,

        -- Parse summarizedExerciseSets → ARRAY<STRUCT>
        ARRAY(
            SELECT
                STRUCT(
                    JSON_VALUE(item, '$.category') AS category,
                    CAST(JSON_VALUE(item, '$.sets') AS INT64) AS sets,
                    CAST(JSON_VALUE(item, '$.reps') AS INT64) AS reps,
                    CAST(JSON_VALUE(item, '$.duration') AS FLOAT64) AS duration_ms,
                    CAST(JSON_VALUE(item, '$.volume') AS FLOAT64) AS volume,
                    CAST(JSON_VALUE(item, '$.maxWeight') AS FLOAT64) AS max_weight_g
                )
            FROM UNNEST(JSON_QUERY_ARRAY(summarizedexercisesets)) AS item
        ) AS summarized_exercise_sets,

        -- Extra fields from summaryDTO not present as flat columns
        CAST(JSON_VALUE(summarydto, '$.minHR') AS FLOAT64) AS min_hr,
        CAST(JSON_VALUE(summarydto, '$.averageMovingSpeed') AS FLOAT64) AS average_moving_speed,
        CAST(JSON_VALUE(summarydto, '$.averageTemperature') AS FLOAT64) AS average_temperature,
        CAST(JSON_VALUE(summarydto, '$.directWorkoutFeel') AS INT64) AS workout_feel,
        CAST(JSON_VALUE(summarydto, '$.directWorkoutRpe') AS INT64) AS workout_rpe,
        CAST(JSON_VALUE(summarydto, '$.totalWork') AS FLOAT64) AS total_work_kj,
        CAST(JSON_VALUE(summarydto, '$.beginPotentialStamina') AS FLOAT64) AS begin_potential_stamina,
        CAST(JSON_VALUE(summarydto, '$.endPotentialStamina') AS FLOAT64) AS end_potential_stamina,
        CAST(JSON_VALUE(summarydto, '$.minAvailableStamina') AS FLOAT64) AS min_available_stamina,
        CAST(JSON_VALUE(summarydto, '$.minPower') AS FLOAT64) AS min_power,

        -- Extra fields from metadataDTO not present as flat columns
        CAST(JSON_VALUE(metadatadto, '$.lastUpdateDate') AS TIMESTAMP) AS last_update_date,
        CAST(JSON_VALUE(metadatadto, '$.uploadedDate') AS TIMESTAMP) AS uploaded_date,
        JSON_VALUE(metadatadto, '$.fileFormat.formatKey') AS file_format_key,
        CAST(JSON_VALUE(metadatadto, '$.hasIntensityIntervals') AS BOOL) AS has_intensity_intervals,
        CAST(JSON_VALUE(metadatadto, '$.hasRunPowerWindData') AS BOOL) AS has_run_power_wind_data

    FROM {{ source('garmin', 'normalized_activity_details') }}
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY activityid
            ORDER BY _ingested_at DESC
        ) AS _row_number
    FROM source
)

SELECT * EXCEPT (_row_number)
FROM deduplicated
WHERE _row_number = 1
