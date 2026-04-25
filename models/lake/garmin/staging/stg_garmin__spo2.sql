{{
    config(
        materialized='view',
        tags=['garmin'],
    )
}}

WITH source AS (
    SELECT
        * EXCEPT (spo2hourlyaverages, spo2valuedescriptorsdtolist),

        -- Parse spO2HourlyAverages → ARRAY<STRUCT>
        array(
            SELECT
                struct(
                    cast(json_value(item, '$.timestamp') AS int64) AS `timestamp`,
                    cast(json_value(item, '$.value') AS int64) AS `value`
                )
            FROM unnest(json_query_array(spo2hourlyaverages)) AS item
        ) AS spo2_hourly_averages

    FROM {{ source('garmin', 'normalized_spo2') }}
),

deduplicated AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY userprofilepk, date
            ORDER BY _ingested_at DESC
        ) AS _row_number
    FROM source
)

SELECT * EXCEPT (_row_number)
FROM deduplicated
WHERE _row_number = 1
