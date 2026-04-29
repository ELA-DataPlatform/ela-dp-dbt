{{
    config(
        materialized='view',
        tags=['garmin'],
    )
}}

WITH source AS (
    SELECT
        * EXCEPT (exercisesets, exercise_sets_data),

        -- Parse exercise_sets_data.exerciseSets → ARRAY<STRUCT>
        -- exerciseSets flat column is always NULL; exercise_sets_data is the actual source.
        ARRAY(
            SELECT
                STRUCT(
                    JSON_VALUE(item, '$.startTime') AS start_time,
                    JSON_VALUE(item, '$.setType') AS set_type,
                    CAST(JSON_VALUE(item, '$.duration') AS FLOAT64) AS duration,
                    CAST(JSON_VALUE(item, '$.repetitionCount') AS INT64) AS repetition_count,
                    CAST(JSON_VALUE(item, '$.weight') AS FLOAT64) AS weight_g,
                    CAST(JSON_VALUE(item, '$.messageIndex') AS INT64) AS message_index,
                    ARRAY(
                        SELECT
                            STRUCT(
                                JSON_VALUE(ex, '$.category') AS category,
                                CAST(JSON_VALUE(ex, '$.probability') AS FLOAT64) AS probability
                            )
                        FROM UNNEST(JSON_QUERY_ARRAY(item, '$.exercises')) AS ex
                    ) AS exercises
                )
            FROM UNNEST(JSON_QUERY_ARRAY(exercise_sets_data, '$.exerciseSets')) AS item
        ) AS exercise_sets

    FROM {{ source('garmin', 'normalized_activity_exercise_sets') }}
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
