{{
    config(
        materialized='view',
        tags=['garmin'],
    )
}}

WITH source AS (
    SELECT
        * EXCEPT (mostrecentvo2max, mostrecenttrainingstatus, mostrecenttrainingloadbalance),

        -- Parse mostRecentVO2Max → generic VO2 max fields (fixed keys)
        cast(json_value(mostrecentvo2max, '$.generic.vo2MaxPreciseValue') AS float64) AS recent_vo2_max_precise,
        cast(json_value(mostrecentvo2max, '$.generic.vo2MaxValue') AS float64) AS recent_vo2_max,
        cast(json_value(mostrecentvo2max, '$.generic.maxMetCategory') AS int64) AS recent_vo2_max_met_category,
        cast(json_value(mostrecentvo2max, '$.heatAltitudeAcclimation.altitudeAcclimation') AS float64)
            AS recent_altitude_acclimation,

        -- Parse mostRecentVO2Max → heat/altitude acclimation fields
        cast(json_value(mostrecentvo2max, '$.heatAltitudeAcclimation.heatAcclimationPercentage') AS float64)
            AS recent_heat_acclimation_pct,
        cast(json_value(mostrecentvo2max, '$.heatAltitudeAcclimation.currentAltitude') AS float64)
            AS recent_current_altitude,
        json_value(mostrecentvo2max, '$.generic.calendarDate') AS recent_vo2_max_date,

        -- Parse mostRecentTrainingStatus → extract latestTrainingStatusData values as ARRAY<STRUCT>
        -- (keys are dynamic deviceIds, use JSON subscript operator for dynamic access)
        array(
            SELECT
                STRUCT(
                    string(ts_data[device_key].calendarDate) AS calendar_date,
                    string(ts_data[device_key].sinceDate) AS since_date,
                    int64(ts_data[device_key].trainingStatus) AS training_status,
                    int64(ts_data[device_key].deviceId) AS device_id,
                    string(ts_data[device_key].sport) AS sport,
                    string(ts_data[device_key].subSport) AS sub_sport,
                    string(ts_data[device_key].fitnessTrendSport) AS fitness_trend_sport,
                    int64(ts_data[device_key].fitnessTrend) AS fitness_trend,
                    string(ts_data[device_key].trainingStatusFeedbackPhrase) AS training_status_feedback_phrase,
                    bool(ts_data[device_key].trainingPaused) AS training_paused,
                    string(ts_data[device_key].acuteTrainingLoadDTO.acwrStatus) AS acwr_status,
                    int64(ts_data[device_key].acuteTrainingLoadDTO.acwrPercent) AS acwr_percent
                )
            FROM
                unnest([json_query(safe.parse_json(mostrecenttrainingstatus), '$.latestTrainingStatusData')])
                    AS ts_data,
                unnest(json_keys(json_query(safe.parse_json(mostrecenttrainingstatus), '$.latestTrainingStatusData')))
                    AS device_key
        ) AS training_status_data,

        -- Parse mostRecentTrainingLoadBalance → extract metricsTrainingLoadBalanceDTOMap values as ARRAY<STRUCT>
        array(
            SELECT
                STRUCT(
                    string(tlb_data[device_key].calendarDate) AS calendar_date,
                    int64(tlb_data[device_key].deviceId) AS device_id,
                    float64(tlb_data[device_key].monthlyLoadAerobicLow) AS monthly_load_aerobic_low,
                    float64(tlb_data[device_key].monthlyLoadAerobicHigh) AS monthly_load_aerobic_high,
                    float64(tlb_data[device_key].monthlyLoadAnaerobic) AS monthly_load_anaerobic,
                    float64(tlb_data[device_key].monthlyLoadAerobicLowTargetMin) AS monthly_load_aerobic_low_target_min,
                    float64(tlb_data[device_key].monthlyLoadAerobicLowTargetMax) AS monthly_load_aerobic_low_target_max,
                    float64(tlb_data[device_key].monthlyLoadAerobicHighTargetMin)
                        AS monthly_load_aerobic_high_target_min,
                    float64(tlb_data[device_key].monthlyLoadAerobicHighTargetMax)
                        AS monthly_load_aerobic_high_target_max,
                    float64(tlb_data[device_key].monthlyLoadAnaerobicTargetMin) AS monthly_load_anaerobic_target_min,
                    float64(tlb_data[device_key].monthlyLoadAnaerobicTargetMax) AS monthly_load_anaerobic_target_max,
                    string(tlb_data[device_key].trainingBalanceFeedbackPhrase) AS training_balance_feedback_phrase,
                    bool(tlb_data[device_key].primaryTrainingDevice) AS primary_training_device
                )
            FROM
                unnest([json_query(
                    safe.parse_json(mostrecenttrainingloadbalance), '$.metricsTrainingLoadBalanceDTOMap'
                )]) AS tlb_data,
                unnest(
                    json_keys(
                        json_query(safe.parse_json(mostrecenttrainingloadbalance), '$.metricsTrainingLoadBalanceDTOMap')
                    )
                ) AS device_key
        ) AS training_load_balance_data

    FROM {{ source('garmin', 'normalized_training_status') }}
),

deduplicated AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY userid, date
            ORDER BY _ingested_at DESC
        ) AS _row_number
    FROM source
)

SELECT * EXCEPT (_row_number)
FROM deduplicated
WHERE _row_number = 1
