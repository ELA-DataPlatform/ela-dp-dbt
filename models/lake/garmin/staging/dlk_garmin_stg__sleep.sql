{{
    config(
        materialized='view',
        tags=['garmin'],
    )
}}

WITH source AS (
    SELECT
        * EXCEPT (
            dailysleepdto, wellnessspo2sleepsummarydto,
            sleeplevels, sleeprestlessmoments,
            sleepheartrate, sleepbodybattery, sleepmovement, sleepstress,
            hrvdata, breathingdisruptiondata,
            wellnessepochrespirationdatadtolist, wellnessepochspo2datadtolist,
            wellnessepochrespirationaverageslist
        ),

        -- Parse sleepLevels → ARRAY<STRUCT>
        cast(json_value(dailysleepdto, '$.id') AS int64) AS sleep_id,

        -- Parse sleepRestlessMoments → ARRAY<STRUCT>
        cast(json_value(dailysleepdto, '$.sleepTimeSeconds') AS int64) AS sleep_time_seconds,

        -- Parse sleepHeartRate → ARRAY<STRUCT>
        cast(json_value(dailysleepdto, '$.napTimeSeconds') AS int64) AS nap_time_seconds,

        -- Parse sleepBodyBattery → ARRAY<STRUCT>
        cast(json_value(dailysleepdto, '$.sleepWindowConfirmed') AS bool) AS sleep_window_confirmed,

        -- Parse sleepMovement → ARRAY<STRUCT>
        cast(json_value(dailysleepdto, '$.deepSleepSeconds') AS int64) AS deep_sleep_seconds,

        -- Parse sleepStress → ARRAY<STRUCT>
        cast(json_value(dailysleepdto, '$.lightSleepSeconds') AS int64) AS light_sleep_seconds,

        -- Parse hrvData → ARRAY<STRUCT>
        cast(json_value(dailysleepdto, '$.remSleepSeconds') AS int64) AS rem_sleep_seconds,

        -- Parse breathingDisruptionData → ARRAY<STRUCT>
        cast(json_value(dailysleepdto, '$.awakeSleepSeconds') AS int64) AS awake_sleep_seconds,

        -- Parse wellnessEpochRespirationDataDTOList → ARRAY<STRUCT>
        cast(json_value(dailysleepdto, '$.unmeasurableSleepSeconds') AS int64) AS unmeasurable_sleep_seconds,

        -- Parse wellnessEpochSPO2DataDTOList → ARRAY<STRUCT>
        cast(json_value(dailysleepdto, '$.averageSpO2Value') AS float64) AS sleep_avg_spo2_value,

        -- Parse dailySleepDTO (JSON object) → sleep summary fields
        cast(json_value(dailysleepdto, '$.lowestSpO2Value') AS int64) AS sleep_lowest_spo2_value,
        cast(json_value(dailysleepdto, '$.highestSpO2Value') AS int64) AS sleep_highest_spo2_value,
        cast(json_value(dailysleepdto, '$.averageSpO2HRSleep') AS float64) AS sleep_avg_spo2_hr,
        cast(json_value(dailysleepdto, '$.averageRespirationValue') AS float64) AS sleep_avg_respiration,
        cast(json_value(dailysleepdto, '$.lowestRespirationValue') AS float64) AS sleep_lowest_respiration,
        cast(json_value(dailysleepdto, '$.highestRespirationValue') AS float64) AS sleep_highest_respiration,
        cast(json_value(dailysleepdto, '$.sleepScoreQualityCheck') AS int64) AS sleep_score_quality_check,
        cast(json_value(dailysleepdto, '$.sleepScores.overall.value') AS int64) AS sleep_overall_score,
        cast(json_value(dailysleepdto, '$.sleepQuality.qualityOrdinal') AS int64) AS sleep_quality_ordinal,
        cast(json_value(dailysleepdto, '$.restlessMomentsCount') AS int64) AS sleep_restless_moments_count,
        cast(json_value(wellnessspo2sleepsummarydto, '$.averageSPO2') AS float64) AS spo2_sleep_avg,
        cast(json_value(wellnessspo2sleepsummarydto, '$.averageSpO2HR') AS float64) AS spo2_sleep_avg_hr,
        cast(json_value(wellnessspo2sleepsummarydto, '$.lowestSPO2') AS int64) AS spo2_sleep_lowest,
        array(
            SELECT
                struct(
                    json_value(item, '$.startGMT') AS start_gmt,
                    json_value(item, '$.endGMT') AS end_gmt,
                    cast(json_value(item, '$.activityLevel') AS float64) AS activity_level
                )
            FROM unnest(json_query_array(sleeplevels)) AS item
        ) AS sleep_levels,
        array(
            SELECT
                struct(
                    cast(json_value(item, '$.startGMT') AS int64) AS start_gmt,
                    cast(json_value(item, '$.value') AS int64) AS `value`
                )
            FROM unnest(json_query_array(sleeprestlessmoments)) AS item
        ) AS sleep_restless_moments,
        array(
            SELECT
                struct(
                    cast(json_value(item, '$.startGMT') AS int64) AS start_gmt,
                    cast(json_value(item, '$.value') AS int64) AS `value`
                )
            FROM unnest(json_query_array(sleepheartrate)) AS item
        ) AS sleep_heart_rate,
        array(
            SELECT
                struct(
                    cast(json_value(item, '$.startGMT') AS int64) AS start_gmt,
                    cast(json_value(item, '$.value') AS int64) AS `value`
                )
            FROM unnest(json_query_array(sleepbodybattery)) AS item
        ) AS sleep_body_battery,
        array(
            SELECT
                struct(
                    json_value(item, '$.startGMT') AS start_gmt,
                    json_value(item, '$.endGMT') AS end_gmt,
                    cast(json_value(item, '$.activityLevel') AS float64) AS activity_level
                )
            FROM unnest(json_query_array(sleepmovement)) AS item
        ) AS sleep_movement,
        array(
            SELECT
                struct(
                    cast(json_value(item, '$.startGMT') AS int64) AS start_gmt,
                    cast(json_value(item, '$.value') AS int64) AS `value`
                )
            FROM unnest(json_query_array(sleepstress)) AS item
        ) AS sleep_stress,
        array(
            SELECT
                struct(
                    cast(json_value(item, '$.startGMT') AS int64) AS start_gmt,
                    cast(json_value(item, '$.value') AS float64) AS `value`
                )
            FROM unnest(json_query_array(hrvdata)) AS item
        ) AS sleep_hrv,
        array(
            SELECT
                struct(
                    cast(json_value(item, '$.startGMT') AS int64) AS start_gmt,
                    cast(json_value(item, '$.endGMT') AS int64) AS end_gmt,
                    cast(json_value(item, '$.value') AS int64) AS `value`
                )
            FROM unnest(json_query_array(breathingdisruptiondata)) AS item
        ) AS sleep_breathing_disruption,
        array(
            SELECT
                struct(
                    cast(json_value(item, '$.startTimeGMT') AS int64) AS start_time_gmt,
                    cast(json_value(item, '$.respirationValue') AS float64) AS respiration_value
                )
            FROM unnest(json_query_array(wellnessepochrespirationdatadtolist)) AS item
        ) AS sleep_respiration,
        array(
            SELECT
                struct(
                    json_value(item, '$.epochTimestamp') AS epoch_timestamp,
                    cast(json_value(item, '$.epochDuration') AS int64) AS epoch_duration,
                    cast(json_value(item, '$.spo2Reading') AS int64) AS spo2_reading,
                    cast(json_value(item, '$.readingConfidence') AS int64) AS reading_confidence,
                    cast(json_value(item, '$.deviceId') AS int64) AS device_id
                )
            FROM unnest(json_query_array(wellnessepochspo2datadtolist)) AS item
        ) AS sleep_spo2,
        array(
            SELECT
                struct(
                    cast(json_value(item, '$.epochEndTimestampGmt') AS int64) AS epoch_end_timestamp_gmt,
                    cast(json_value(item, '$.respirationAverageValue') AS float64) AS respiration_average_value,
                    cast(json_value(item, '$.respirationHighValue') AS float64) AS respiration_high_value,
                    cast(json_value(item, '$.respirationLowValue') AS float64) AS respiration_low_value
                )
            FROM unnest(json_query_array(wellnessepochrespirationaverageslist)) AS item
        ) AS sleep_respiration_averages,
        json_value(dailysleepdto, '$.calendarDate') AS sleep_calendar_date,
        json_value(dailysleepdto, '$.sleepWindowConfirmationType') AS sleep_window_confirmation_type,
        json_value(dailysleepdto, '$.sleepStartTimestampGMT') AS sleep_start_timestamp_gmt,
        json_value(dailysleepdto, '$.sleepEndTimestampGMT') AS sleep_end_timestamp_gmt,
        json_value(dailysleepdto, '$.sleepStartTimestampLocal') AS sleep_start_timestamp_local,
        json_value(dailysleepdto, '$.sleepEndTimestampLocal') AS sleep_end_timestamp_local,
        json_value(dailysleepdto, '$.autoSleepStartTimestampGMT') AS auto_sleep_start_timestamp_gmt,
        json_value(dailysleepdto, '$.autoSleepEndTimestampGMT') AS auto_sleep_end_timestamp_gmt,

        -- Parse wellnessSpO2SleepSummaryDTO (JSON object) → SpO2 summary
        json_value(dailysleepdto, '$.sleepScoreFeedback') AS sleep_score_feedback,
        json_value(dailysleepdto, '$.sleepScores.overall.qualifierKey') AS sleep_overall_score_qualifier,
        json_value(dailysleepdto, '$.sleepQuality.key') AS sleep_quality_key,
        json_value(wellnessspo2sleepsummarydto, '$.sleepMeasurementStartGMT') AS spo2_sleep_measurement_start_gmt,
        json_value(wellnessspo2sleepsummarydto, '$.sleepMeasurementEndGMT') AS spo2_sleep_measurement_end_gmt

    FROM {{ source('garmin', 'normalized_sleep') }}
),

deduplicated AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY date
            ORDER BY _ingested_at DESC
        ) AS _row_number
    FROM source
)

SELECT * EXCEPT (_row_number)
FROM deduplicated
WHERE _row_number = 1
