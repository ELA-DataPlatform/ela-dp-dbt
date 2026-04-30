{{
    config(
        materialized='view',
        tags=['garmin'],
    )
}}

WITH source AS (
    SELECT
        * EXCEPT (data_type, calendardate),
        -- Surrogate key: handles intraday rows (startGMT populated) and daily
        -- summary rows (startGMT NULL) so the service MERGE can always match.
        -- NULL = NULL is false in SQL joins, which would cause duplicates without this.
        CONCAT(
            CAST(date AS STRING),
            '|',
            COALESCE(FORMAT_TIMESTAMP('%Y%m%dT%H%M%S', startgmt), 'DAILY')
        ) AS step_key
    FROM {{ source('garmin', 'normalized_steps') }}
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY step_key
            ORDER BY _ingested_at DESC
        ) AS _row_number
    FROM source
)

SELECT * EXCEPT (_row_number)
FROM deduplicated
WHERE _row_number = 1
