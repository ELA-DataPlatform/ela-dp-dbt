
{{
    config(
        materialized='view',
        tags=['garmin'],
    )
}}

with source as (
    select
        * except(enduranceScoreDTO),

        -- Parse enduranceScoreDTO (JSON object) → endurance score fields
        cast(json_value(enduranceScoreDTO, '$.overallScore') as float64) as endurance_overall_score,
        cast(json_value(enduranceScoreDTO, '$.classification') as int64) as endurance_classification,
        cast(json_value(enduranceScoreDTO, '$.feedbackPhrase') as int64) as endurance_feedback_phrase,
        cast(json_value(enduranceScoreDTO, '$.gaugeLowerLimit') as float64) as endurance_gauge_lower_limit,
        cast(json_value(enduranceScoreDTO, '$.gaugeUpperLimit') as float64) as endurance_gauge_upper_limit,
        cast(json_value(enduranceScoreDTO, '$.classificationLowerLimitIntermediate') as float64) as endurance_class_intermediate,
        cast(json_value(enduranceScoreDTO, '$.classificationLowerLimitTrained') as float64) as endurance_class_trained,
        cast(json_value(enduranceScoreDTO, '$.classificationLowerLimitWellTrained') as float64) as endurance_class_well_trained,
        cast(json_value(enduranceScoreDTO, '$.classificationLowerLimitExpert') as float64) as endurance_class_expert,
        cast(json_value(enduranceScoreDTO, '$.classificationLowerLimitSuperior') as float64) as endurance_class_superior,
        cast(json_value(enduranceScoreDTO, '$.classificationLowerLimitElite') as float64) as endurance_class_elite,
        json_query(enduranceScoreDTO, '$.contributors') as endurance_contributors

    from {{ source('garmin', 'normalized_endurance_score') }}
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by userProfilePK, startDate
            order by _ingested_at desc
        ) as _row_number
    from source
)

select * except(_row_number)
from deduplicated
where _row_number = 1
