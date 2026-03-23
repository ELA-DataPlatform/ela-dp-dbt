
{{
    config(
        materialized='view',
        tags=['garmin'],
    )
}}

with source as (
    select
        * except(dailySleepDTO, wellnessSpO2SleepSummaryDTO,
                 sleepLevels, sleepRestlessMoments,
                 sleepHeartRate, sleepBodyBattery, sleepMovement, sleepStress,
                 hrvData, breathingDisruptionData,
                 wellnessEpochRespirationDataDTOList, wellnessEpochSPO2DataDTOList),

        -- Parse sleepLevels → ARRAY<STRUCT>
        array(
            select struct(
                json_value(item, '$.startGMT') as start_gmt,
                json_value(item, '$.endGMT') as end_gmt,
                cast(json_value(item, '$.activityLevel') as float64) as activity_level
            )
            from unnest(json_query_array(sleepLevels)) as item
        ) as sleep_levels,

        -- Parse sleepRestlessMoments → ARRAY<STRUCT>
        array(
            select struct(
                cast(json_value(item, '$.startGMT') as int64) as start_gmt,
                cast(json_value(item, '$.value') as int64) as value
            )
            from unnest(json_query_array(sleepRestlessMoments)) as item
        ) as sleep_restless_moments,

        -- Parse sleepHeartRate → ARRAY<STRUCT>
        array(
            select struct(
                cast(json_value(item, '$.startGMT') as int64) as start_gmt,
                cast(json_value(item, '$.value') as int64) as value
            )
            from unnest(json_query_array(sleepHeartRate)) as item
        ) as sleep_heart_rate,

        -- Parse sleepBodyBattery → ARRAY<STRUCT>
        array(
            select struct(
                cast(json_value(item, '$.startGMT') as int64) as start_gmt,
                cast(json_value(item, '$.value') as int64) as value
            )
            from unnest(json_query_array(sleepBodyBattery)) as item
        ) as sleep_body_battery,

        -- Parse sleepMovement → ARRAY<STRUCT>
        array(
            select struct(
                json_value(item, '$.startGMT') as start_gmt,
                json_value(item, '$.endGMT') as end_gmt,
                cast(json_value(item, '$.activityLevel') as float64) as activity_level
            )
            from unnest(json_query_array(sleepMovement)) as item
        ) as sleep_movement,

        -- Parse sleepStress → ARRAY<STRUCT>
        array(
            select struct(
                cast(json_value(item, '$.startGMT') as int64) as start_gmt,
                cast(json_value(item, '$.value') as int64) as value
            )
            from unnest(json_query_array(sleepStress)) as item
        ) as sleep_stress,

        -- Parse hrvData → ARRAY<STRUCT>
        array(
            select struct(
                cast(json_value(item, '$.startGMT') as int64) as start_gmt,
                cast(json_value(item, '$.value') as float64) as value
            )
            from unnest(json_query_array(hrvData)) as item
        ) as sleep_hrv,

        -- Parse breathingDisruptionData → ARRAY<STRUCT>
        array(
            select struct(
                cast(json_value(item, '$.startGMT') as int64) as start_gmt,
                cast(json_value(item, '$.endGMT') as int64) as end_gmt,
                cast(json_value(item, '$.value') as int64) as value
            )
            from unnest(json_query_array(breathingDisruptionData)) as item
        ) as sleep_breathing_disruption,

        -- Parse wellnessEpochRespirationDataDTOList → ARRAY<STRUCT>
        array(
            select struct(
                cast(json_value(item, '$.startTimeGMT') as int64) as start_time_gmt,
                cast(json_value(item, '$.respirationValue') as float64) as respiration_value
            )
            from unnest(json_query_array(wellnessEpochRespirationDataDTOList)) as item
        ) as sleep_respiration,

        -- Parse wellnessEpochSPO2DataDTOList → ARRAY<STRUCT>
        array(
            select struct(
                json_value(item, '$.epochTimestamp') as epoch_timestamp,
                cast(json_value(item, '$.epochDuration') as int64) as epoch_duration,
                cast(json_value(item, '$.spo2Reading') as int64) as spo2_reading,
                cast(json_value(item, '$.readingConfidence') as int64) as reading_confidence,
                cast(json_value(item, '$.deviceId') as int64) as device_id
            )
            from unnest(json_query_array(wellnessEpochSPO2DataDTOList)) as item
        ) as sleep_spo2,

        -- Parse dailySleepDTO (JSON object) → sleep summary fields
        cast(json_value(dailySleepDTO, '$.id') as int64) as sleep_id,
        json_value(dailySleepDTO, '$.calendarDate') as sleep_calendar_date,
        cast(json_value(dailySleepDTO, '$.sleepTimeSeconds') as int64) as sleep_time_seconds,
        cast(json_value(dailySleepDTO, '$.napTimeSeconds') as int64) as nap_time_seconds,
        cast(json_value(dailySleepDTO, '$.sleepWindowConfirmed') as bool) as sleep_window_confirmed,
        json_value(dailySleepDTO, '$.sleepWindowConfirmationType') as sleep_window_confirmation_type,
        json_value(dailySleepDTO, '$.sleepStartTimestampGMT') as sleep_start_timestamp_gmt,
        json_value(dailySleepDTO, '$.sleepEndTimestampGMT') as sleep_end_timestamp_gmt,
        json_value(dailySleepDTO, '$.sleepStartTimestampLocal') as sleep_start_timestamp_local,
        json_value(dailySleepDTO, '$.sleepEndTimestampLocal') as sleep_end_timestamp_local,
        json_value(dailySleepDTO, '$.autoSleepStartTimestampGMT') as auto_sleep_start_timestamp_gmt,
        json_value(dailySleepDTO, '$.autoSleepEndTimestampGMT') as auto_sleep_end_timestamp_gmt,
        cast(json_value(dailySleepDTO, '$.deepSleepSeconds') as int64) as deep_sleep_seconds,
        cast(json_value(dailySleepDTO, '$.lightSleepSeconds') as int64) as light_sleep_seconds,
        cast(json_value(dailySleepDTO, '$.remSleepSeconds') as int64) as rem_sleep_seconds,
        cast(json_value(dailySleepDTO, '$.awakeSleepSeconds') as int64) as awake_sleep_seconds,
        cast(json_value(dailySleepDTO, '$.unmeasurableSleepSeconds') as int64) as unmeasurable_sleep_seconds,
        cast(json_value(dailySleepDTO, '$.averageSpO2Value') as float64) as sleep_avg_spo2_value,
        cast(json_value(dailySleepDTO, '$.lowestSpO2Value') as int64) as sleep_lowest_spo2_value,
        cast(json_value(dailySleepDTO, '$.highestSpO2Value') as int64) as sleep_highest_spo2_value,
        cast(json_value(dailySleepDTO, '$.averageSpO2HRSleep') as float64) as sleep_avg_spo2_hr,
        cast(json_value(dailySleepDTO, '$.averageRespirationValue') as float64) as sleep_avg_respiration,
        cast(json_value(dailySleepDTO, '$.lowestRespirationValue') as float64) as sleep_lowest_respiration,
        cast(json_value(dailySleepDTO, '$.highestRespirationValue') as float64) as sleep_highest_respiration,
        cast(json_value(dailySleepDTO, '$.sleepScoreQualityCheck') as int64) as sleep_score_quality_check,
        json_value(dailySleepDTO, '$.sleepScoreFeedback') as sleep_score_feedback,
        cast(json_value(dailySleepDTO, '$.overallScore.value') as int64) as sleep_overall_score,
        json_value(dailySleepDTO, '$.overallScore.qualifierKey') as sleep_overall_score_qualifier,
        cast(json_value(dailySleepDTO, '$.sleepQuality.qualityOrdinal') as int64) as sleep_quality_ordinal,
        json_value(dailySleepDTO, '$.sleepQuality.key') as sleep_quality_key,
        cast(json_value(dailySleepDTO, '$.restlessMomentsCount') as int64) as sleep_restless_moments_count,

        -- Parse wellnessSpO2SleepSummaryDTO (JSON object) → SpO2 summary
        cast(json_value(wellnessSpO2SleepSummaryDTO, '$.averageSPO2') as float64) as spo2_sleep_avg,
        cast(json_value(wellnessSpO2SleepSummaryDTO, '$.averageSpO2HR') as float64) as spo2_sleep_avg_hr,
        cast(json_value(wellnessSpO2SleepSummaryDTO, '$.lowestSPO2') as int64) as spo2_sleep_lowest,
        json_value(wellnessSpO2SleepSummaryDTO, '$.sleepMeasurementStartGMT') as spo2_sleep_measurement_start_gmt,
        json_value(wellnessSpO2SleepSummaryDTO, '$.sleepMeasurementEndGMT') as spo2_sleep_measurement_end_gmt

    from {{ source('garmin', 'normalized_sleep') }}
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by date
            order by _ingested_at desc
        ) as _row_number
    from source
)

select * except(_row_number)
from deduplicated
where _row_number = 1
