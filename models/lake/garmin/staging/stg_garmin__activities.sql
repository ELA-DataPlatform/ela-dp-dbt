{{
    config(
        materialized='view',
        tags=['garmin'],
    )
}}

WITH source AS (
    SELECT
        * EXCEPT (splitsummaries, summarizedexercisesets, summarizeddiveinfo),

        -- Parse splitSummaries → ARRAY<STRUCT>
        array(
            SELECT
                struct(
                    json_value(item, '$.splitType') AS split_type,
                    cast(json_value(item, '$.noOfSplits') AS int64) AS no_of_splits,
                    cast(json_value(item, '$.distance') AS float64) AS distance,
                    cast(json_value(item, '$.duration') AS float64) AS duration,
                    cast(json_value(item, '$.totalAscent') AS float64) AS total_ascent,
                    cast(json_value(item, '$.elevationLoss') AS float64) AS elevation_loss,
                    cast(json_value(item, '$.averageSpeed') AS float64) AS average_speed,
                    cast(json_value(item, '$.maxSpeed') AS float64) AS max_speed,
                    cast(json_value(item, '$.maxElevationGain') AS float64) AS max_elevation_gain,
                    cast(json_value(item, '$.averageElevationGain') AS float64) AS average_elevation_gain,
                    cast(json_value(item, '$.maxDistance') AS float64) AS max_distance,
                    cast(json_value(item, '$.numClimbSends') AS int64) AS num_climb_sends,
                    cast(json_value(item, '$.numFalls') AS int64) AS num_falls
                )
            FROM unnest(json_query_array(splitsummaries)) AS item
        ) AS split_summaries

    FROM {{ source('garmin', 'normalized_activities') }}
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
