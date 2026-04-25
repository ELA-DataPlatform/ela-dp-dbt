{{
    config(
        materialized='view',
        tags=['garmin'],
    )
}}

WITH source AS (
    SELECT
        * EXCEPT (endurancescoredto),

        -- Parse enduranceScoreDTO (JSON object) → endurance score fields
        cast(json_value(endurancescoredto, '$.overallScore') AS float64) AS endurance_overall_score,
        cast(json_value(endurancescoredto, '$.classification') AS int64) AS endurance_classification,
        cast(json_value(endurancescoredto, '$.feedbackPhrase') AS int64) AS endurance_feedback_phrase,
        cast(json_value(endurancescoredto, '$.gaugeLowerLimit') AS float64) AS endurance_gauge_lower_limit,
        cast(json_value(endurancescoredto, '$.gaugeUpperLimit') AS float64) AS endurance_gauge_upper_limit,
        cast(json_value(endurancescoredto, '$.classificationLowerLimitIntermediate') AS float64)
            AS endurance_class_intermediate,
        cast(json_value(endurancescoredto, '$.classificationLowerLimitTrained') AS float64) AS endurance_class_trained,
        cast(json_value(endurancescoredto, '$.classificationLowerLimitWellTrained') AS float64)
            AS endurance_class_well_trained,
        cast(json_value(endurancescoredto, '$.classificationLowerLimitExpert') AS float64) AS endurance_class_expert,
        cast(json_value(endurancescoredto, '$.classificationLowerLimitSuperior') AS float64)
            AS endurance_class_superior,
        cast(json_value(endurancescoredto, '$.classificationLowerLimitElite') AS float64) AS endurance_class_elite,
        json_query(endurancescoredto, '$.contributors') AS endurance_contributors

    FROM {{ source('garmin', 'normalized_endurance_score') }}
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
