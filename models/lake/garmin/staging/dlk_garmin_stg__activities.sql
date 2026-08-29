{{
    config(
        materialized='view',
        tags=['garmin'],
    )
}}

WITH source AS (
    SELECT
        * EXCEPT (
            splitsummaries, summarizedexercisesets, summarizeddiveinfo, activitytype, eventtype, privacy, userroles
        ),

        -- Parse activityType → STRUCT
        STRUCT(
            CAST(JSON_VALUE(activitytype, '$.typeId') AS INT64) AS type_id,
            JSON_VALUE(activitytype, '$.typeKey') AS type_key,
            CAST(JSON_VALUE(activitytype, '$.parentTypeId') AS INT64) AS parent_type_id,
            CAST(JSON_VALUE(activitytype, '$.isHidden') AS BOOL) AS is_hidden,
            CAST(JSON_VALUE(activitytype, '$.restricted') AS BOOL) AS restricted,
            CAST(JSON_VALUE(activitytype, '$.trimmable') AS BOOL) AS trimmable
        ) AS activity_type,

        -- Parse eventType → STRUCT
        STRUCT(
            CAST(JSON_VALUE(eventtype, '$.typeId') AS INT64) AS type_id,
            JSON_VALUE(eventtype, '$.typeKey') AS type_key,
            CAST(JSON_VALUE(eventtype, '$.sortOrder') AS INT64) AS sort_order
        ) AS event_type,

        -- Parse privacy → STRUCT
        STRUCT(
            CAST(JSON_VALUE(privacy, '$.typeId') AS INT64) AS type_id,
            JSON_VALUE(privacy, '$.typeKey') AS type_key
        ) AS privacy,

        -- Parse userRoles → ARRAY<STRING>
        ARRAY(
            SELECT JSON_VALUE(role)
            FROM UNNEST(JSON_QUERY_ARRAY(userroles)) AS `role`
        ) AS user_roles,

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
        ) AS split_summaries

    FROM {{ source('garmin', 'normalized_activities') }}
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
