{{
    config(
        materialized='view',
        tags=['garmin'],
    )
}}

WITH source AS (
    SELECT
        * EXCEPT (hr_zones_data),

        -- Parse hr_zones_data → ARRAY<STRUCT>
        array(
            SELECT
                struct(
                    cast(json_value(item, '$.zoneNumber') AS int64) AS zone_number,
                    cast(json_value(item, '$.secsInZone') AS float64) AS secs_in_zone,
                    cast(json_value(item, '$.zoneLowBoundary') AS int64) AS zone_low_boundary
                )
            FROM unnest(json_query_array(hr_zones_data)) AS item
        ) AS hr_zones

    FROM {{ source('garmin', 'normalized_activity_hr_zones') }}
),

deduplicated AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY activityid
            ORDER BY _ingested_at DESC
        ) AS _row_number
    FROM source
)

SELECT * EXCEPT (_row_number)
FROM deduplicated
WHERE _row_number = 1
