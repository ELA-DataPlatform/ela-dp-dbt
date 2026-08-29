{{
    config(
        materialized='view',
        tags=['garmin'],
    )
}}

WITH source AS (
    SELECT
        * EXCEPT (hillscoredtolist),

        -- Parse hillScoreDTOList → ARRAY<STRUCT>
        array(
            SELECT
                STRUCT(
                    cast(json_value(item, '$.userProfilePK') AS INT64) AS user_profile_pk,
                    cast(json_value(item, '$.deviceId') AS INT64) AS device_id,
                    json_value(item, '$.calendarDate') AS calendar_date,
                    cast(json_value(item, '$.strengthScore') AS INT64) AS strength_score,
                    cast(json_value(item, '$.enduranceScore') AS INT64) AS endurance_score,
                    cast(json_value(item, '$.hillScoreClassificationId') AS INT64) AS hill_score_classification_id,
                    cast(json_value(item, '$.overallScore') AS INT64) AS overall_score,
                    cast(json_value(item, '$.hillScoreFeedbackPhraseId') AS INT64) AS hill_score_feedback_phrase_id,
                    cast(json_value(item, '$.primaryTrainingDevice') AS BOOL) AS primary_training_device
                )
            FROM unnest(json_query_array(hillscoredtolist)) AS item
        ) AS hill_scores

    FROM {{ source('garmin', 'normalized_hill_score') }}
),

deduplicated AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY userprofilepk, startdate
            ORDER BY _ingested_at DESC
        ) AS _row_number
    FROM source
)

SELECT * EXCEPT (_row_number)
FROM deduplicated
WHERE _row_number = 1
