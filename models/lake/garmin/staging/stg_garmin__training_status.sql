
{{
    config(
        materialized='view',
        tags=['garmin'],
    )
}}

with source as (
    select
        * except(mostRecentVO2Max, mostRecentTrainingStatus, mostRecentTrainingLoadBalance),

        -- Parse mostRecentVO2Max → generic VO2 max fields (fixed keys)
        cast(json_value(mostRecentVO2Max, '$.generic.vo2MaxPreciseValue') as float64) as recent_vo2_max_precise,
        cast(json_value(mostRecentVO2Max, '$.generic.vo2MaxValue') as float64) as recent_vo2_max,
        json_value(mostRecentVO2Max, '$.generic.calendarDate') as recent_vo2_max_date,
        cast(json_value(mostRecentVO2Max, '$.generic.maxMetCategory') as int64) as recent_vo2_max_met_category,

        -- Parse mostRecentVO2Max → heat/altitude acclimation fields
        cast(json_value(mostRecentVO2Max, '$.heatAltitudeAcclimation.altitudeAcclimation') as float64) as recent_altitude_acclimation,
        cast(json_value(mostRecentVO2Max, '$.heatAltitudeAcclimation.heatAcclimationPercentage') as float64) as recent_heat_acclimation_pct,
        cast(json_value(mostRecentVO2Max, '$.heatAltitudeAcclimation.currentAltitude') as float64) as recent_current_altitude,

        -- Parse mostRecentTrainingStatus → extract latestTrainingStatusData values as ARRAY<STRUCT>
        -- (keys are dynamic deviceIds, use JSON subscript operator for dynamic access)
        array(
            select struct(
                string(ts_data[device_key].calendarDate) as calendar_date,
                string(ts_data[device_key].sinceDate) as since_date,
                int64(ts_data[device_key].trainingStatus) as training_status,
                int64(ts_data[device_key].deviceId) as device_id,
                string(ts_data[device_key].sport) as sport,
                string(ts_data[device_key].subSport) as sub_sport,
                string(ts_data[device_key].fitnessTrendSport) as fitness_trend_sport,
                int64(ts_data[device_key].fitnessTrend) as fitness_trend,
                string(ts_data[device_key].trainingStatusFeedbackPhrase) as training_status_feedback_phrase,
                bool(ts_data[device_key].trainingPaused) as training_paused,
                string(ts_data[device_key].acuteTrainingLoadDTO.acwrStatus) as acwr_status,
                int64(ts_data[device_key].acuteTrainingLoadDTO.acwrPercent) as acwr_percent
            )
            from unnest([json_query(safe.parse_json(mostRecentTrainingStatus), '$.latestTrainingStatusData')]) as ts_data,
            unnest(json_keys(json_query(safe.parse_json(mostRecentTrainingStatus), '$.latestTrainingStatusData'))) as device_key
        ) as training_status_data,

        -- Parse mostRecentTrainingLoadBalance → extract metricsTrainingLoadBalanceDTOMap values as ARRAY<STRUCT>
        array(
            select struct(
                string(tlb_data[device_key].calendarDate) as calendar_date,
                int64(tlb_data[device_key].deviceId) as device_id,
                float64(tlb_data[device_key].monthlyLoadAerobicLow) as monthly_load_aerobic_low,
                float64(tlb_data[device_key].monthlyLoadAerobicHigh) as monthly_load_aerobic_high,
                float64(tlb_data[device_key].monthlyLoadAnaerobic) as monthly_load_anaerobic,
                float64(tlb_data[device_key].monthlyLoadAerobicLowTargetMin) as monthly_load_aerobic_low_target_min,
                float64(tlb_data[device_key].monthlyLoadAerobicLowTargetMax) as monthly_load_aerobic_low_target_max,
                float64(tlb_data[device_key].monthlyLoadAerobicHighTargetMin) as monthly_load_aerobic_high_target_min,
                float64(tlb_data[device_key].monthlyLoadAerobicHighTargetMax) as monthly_load_aerobic_high_target_max,
                float64(tlb_data[device_key].monthlyLoadAnaerobicTargetMin) as monthly_load_anaerobic_target_min,
                float64(tlb_data[device_key].monthlyLoadAnaerobicTargetMax) as monthly_load_anaerobic_target_max,
                string(tlb_data[device_key].trainingBalanceFeedbackPhrase) as training_balance_feedback_phrase,
                bool(tlb_data[device_key].primaryTrainingDevice) as primary_training_device
            )
            from unnest([json_query(safe.parse_json(mostRecentTrainingLoadBalance), '$.metricsTrainingLoadBalanceDTOMap')]) as tlb_data,
            unnest(json_keys(json_query(safe.parse_json(mostRecentTrainingLoadBalance), '$.metricsTrainingLoadBalanceDTOMap'))) as device_key
        ) as training_load_balance_data

    from {{ source('garmin', 'normalized_training_status') }}
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by userId, date
            order by _ingested_at desc
        ) as _row_number
    from source
)

select * except(_row_number)
from deduplicated
where _row_number = 1
