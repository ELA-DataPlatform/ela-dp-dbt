{{
    config(
        materialized='view',
        tags=['garmin'],
    )
}}

WITH source AS (
    SELECT
        * EXCEPT (endurancescoredto, contributors, groupmap),

        -- Parse enduranceScoreDTO (JSON object) → endurance score fields
        CAST(JSON_VALUE(endurancescoredto, '$.overallScore') AS FLOAT64) AS endurance_overall_score,
        CAST(JSON_VALUE(endurancescoredto, '$.classification') AS INT64) AS endurance_classification,
        CAST(JSON_VALUE(endurancescoredto, '$.feedbackPhrase') AS INT64) AS endurance_feedback_phrase,
        CAST(JSON_VALUE(endurancescoredto, '$.gaugeLowerLimit') AS FLOAT64) AS endurance_gauge_lower_limit,
        CAST(JSON_VALUE(endurancescoredto, '$.gaugeUpperLimit') AS FLOAT64) AS endurance_gauge_upper_limit,
        CAST(JSON_VALUE(endurancescoredto, '$.classificationLowerLimitIntermediate') AS FLOAT64)
            AS endurance_class_intermediate,
        CAST(JSON_VALUE(endurancescoredto, '$.classificationLowerLimitTrained') AS FLOAT64) AS endurance_class_trained,
        CAST(JSON_VALUE(endurancescoredto, '$.classificationLowerLimitWellTrained') AS FLOAT64)
            AS endurance_class_well_trained,
        CAST(JSON_VALUE(endurancescoredto, '$.classificationLowerLimitExpert') AS FLOAT64) AS endurance_class_expert,
        CAST(JSON_VALUE(endurancescoredto, '$.classificationLowerLimitSuperior') AS FLOAT64)
            AS endurance_class_superior,
        CAST(JSON_VALUE(endurancescoredto, '$.classificationLowerLimitElite') AS FLOAT64) AS endurance_class_elite,

        -- Parse contributors (flat JSON column) → ARRAY<STRUCT>
        -- groupMap excluded: dynamic date-keyed structure unsuitable for a fixed typed schema.
        ARRAY(
            SELECT
                STRUCT(
                    CAST(JSON_VALUE(item, '$.activityTypeId') AS INT64) AS activity_type_id,
                    CAST(JSON_VALUE(item, '$.group') AS INT64) AS group_id,
                    CAST(JSON_VALUE(item, '$.contribution') AS FLOAT64) AS contribution
                )
            FROM UNNEST(JSON_QUERY_ARRAY(contributors)) AS item
        ) AS endurance_contributors

    FROM {{ source('garmin', 'normalized_endurance_score') }}
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY userprofilepk, startdate
            ORDER BY _ingested_at DESC
        ) AS _row_number
    FROM source
)

SELECT * EXCEPT (_row_number)
FROM deduplicated
WHERE _row_number = 1
