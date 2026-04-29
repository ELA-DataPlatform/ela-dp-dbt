{{
    config(
        materialized='view',
        tags=['garmin'],
    )
}}

WITH source AS (
    SELECT
        * EXCEPT (spo2valuesarray, spo2hourlyaverages, spo2valuedescriptorsdtolist),

        -- Parse spo2ValuesArray → indexed format [timestamp, spo2Reading, readingConfidence]
        ARRAY(
            SELECT
                STRUCT(
                    CAST(JSON_VALUE(item, '$[0]') AS INT64) AS timestamp,
                    CAST(JSON_VALUE(item, '$[1]') AS INT64) AS spo2_reading,
                    CAST(JSON_VALUE(item, '$[2]') AS INT64) AS reading_confidence
                )
            FROM UNNEST(JSON_QUERY_ARRAY(spo2valuesarray)) AS item
        ) AS spo2_values,

        -- Parse spO2HourlyAverages → named keys {"timestamp": ..., "value": ...}
        ARRAY(
            SELECT
                STRUCT(
                    CAST(JSON_VALUE(item, '$.timestamp') AS INT64) AS timestamp,
                    CAST(JSON_VALUE(item, '$.value') AS INT64) AS value
                )
            FROM UNNEST(JSON_QUERY_ARRAY(spo2hourlyaverages)) AS item
        ) AS spo2_hourly_averages

    FROM {{ source('garmin', 'normalized_spo2') }}
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY userprofilepk, date
            ORDER BY _ingested_at DESC
        ) AS _row_number
    FROM source
)

SELECT * EXCEPT (_row_number)
FROM deduplicated
WHERE _row_number = 1
