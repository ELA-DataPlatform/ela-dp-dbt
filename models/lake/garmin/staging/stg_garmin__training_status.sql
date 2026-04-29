{{
    config(
        materialized='view',
        tags=['garmin'],
    )
}}

WITH source AS (
    SELECT * FROM {{ source('garmin', 'normalized_training_status') }}
),

parsed AS (
    SELECT
        _ingested_at,
        date,
        userid,
        lastprimarysyncdate,
        showselector,
        recordeddevices,
        SAFE.PARSE_JSON(latesttrainingstatusdata) AS _ts_json
    FROM source
),

extracted AS (
    SELECT
        * EXCEPT (_ts_json),
        _ts_json[JSON_KEYS(_ts_json)[OFFSET(0)]] AS _device
    FROM parsed
),

deduplicated AS (
    SELECT
        _ingested_at,
        date,
        userid,
        lastprimarysyncdate,
        showselector,
        recordeddevices,
        CAST(JSON_VALUE(_device, '$.deviceId') AS INT64)                                  AS device_id,
        JSON_VALUE(_device, '$.calendarDate')                                             AS calendar_date,
        JSON_VALUE(_device, '$.sinceDate')                                                AS since_date,
        CAST(JSON_VALUE(_device, '$.trainingStatus') AS INT64)                            AS training_status,
        JSON_VALUE(_device, '$.sport')                                                    AS sport,
        JSON_VALUE(_device, '$.subSport')                                                 AS sub_sport,
        JSON_VALUE(_device, '$.fitnessTrendSport')                                        AS fitness_trend_sport,
        CAST(JSON_VALUE(_device, '$.fitnessTrend') AS INT64)                              AS fitness_trend,
        JSON_VALUE(_device, '$.trainingStatusFeedbackPhrase')                             AS training_status_feedback_phrase,
        CAST(JSON_VALUE(_device, '$.trainingPaused') AS BOOL)                             AS training_paused,
        CAST(JSON_VALUE(_device, '$.acuteTrainingLoadDTO.acwrPercent') AS INT64)          AS acwr_percent,
        JSON_VALUE(_device, '$.acuteTrainingLoadDTO.acwrStatus')                          AS acwr_status,
        JSON_VALUE(_device, '$.acuteTrainingLoadDTO.acwrStatusFeedback')                  AS acwr_status_feedback,
        CAST(JSON_VALUE(_device, '$.acuteTrainingLoadDTO.dailyTrainingLoadAcute') AS INT64)
            AS daily_training_load_acute,
        CAST(JSON_VALUE(_device, '$.acuteTrainingLoadDTO.dailyTrainingLoadChronic') AS INT64)
            AS daily_training_load_chronic,
        CAST(JSON_VALUE(_device, '$.acuteTrainingLoadDTO.maxTrainingLoadChronic') AS FLOAT64)
            AS max_training_load_chronic,
        CAST(JSON_VALUE(_device, '$.acuteTrainingLoadDTO.minTrainingLoadChronic') AS FLOAT64)
            AS min_training_load_chronic,
        CAST(JSON_VALUE(_device, '$.acuteTrainingLoadDTO.dailyAcuteChronicWorkloadRatio') AS FLOAT64)
            AS daily_acute_chronic_workload_ratio,
        ROW_NUMBER() OVER (
            PARTITION BY userid, date
            ORDER BY _ingested_at DESC
        ) AS _row_number
    FROM extracted
)

SELECT * EXCEPT (_row_number)
FROM deduplicated
WHERE _row_number = 1
