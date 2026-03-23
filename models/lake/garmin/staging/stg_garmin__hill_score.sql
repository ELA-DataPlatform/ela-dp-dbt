
{{
    config(
        materialized='view',
        tags=['garmin'],
    )
}}

with source as (
    select
        * except(hillScoreDTOList),

        -- Parse hillScoreDTOList → ARRAY<STRUCT>
        array(
            select struct(
                cast(json_value(item, '$.userProfilePK') as int64) as user_profile_pk,
                cast(json_value(item, '$.deviceId') as int64) as device_id,
                json_value(item, '$.calendarDate') as calendar_date,
                cast(json_value(item, '$.strengthScore') as int64) as strength_score,
                cast(json_value(item, '$.enduranceScore') as int64) as endurance_score,
                cast(json_value(item, '$.hillScoreClassificationId') as int64) as hill_score_classification_id,
                cast(json_value(item, '$.overallScore') as int64) as overall_score,
                cast(json_value(item, '$.hillScoreFeedbackPhraseId') as int64) as hill_score_feedback_phrase_id,
                cast(json_value(item, '$.primaryTrainingDevice') as bool) as primary_training_device
            )
            from unnest(json_query_array(hillScoreDTOList)) as item
        ) as hill_scores

    from {{ source('garmin', 'normalized_hill_score') }}
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
