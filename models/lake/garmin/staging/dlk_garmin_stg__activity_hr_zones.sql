{{
    config(
        materialized='view',
        tags=['garmin'],
    )
}}

-- Normalized table has one row per zone per activity.
-- Deduplicate per (activityId, zoneNumber) first, then aggregate all zones
-- into a single ARRAY<STRUCT> per activity.
-- Flat columns (zoneNumber / secsInZone / zoneLowBoundary) are the primary
-- source; the hr_zones_data JSON blob is used as a fallback for legacy records.

WITH

deduped_zones AS (
    SELECT
        activityid,
        activityname,
        activitytype,
        starttimelocal,
        data_type,
        _ingested_at,
        zonenumber,
        secsinzone,
        zonelowboundary,
        hr_zones_data,
        row_number() OVER (
            PARTITION BY activityid, zonenumber
            ORDER BY _ingested_at DESC
        ) AS _row_number
    FROM {{ source('garmin', 'normalized_activity_hr_zones') }}
),

zones AS (
    SELECT * FROM deduped_zones
    WHERE _row_number = 1
),

aggregated AS (
    SELECT
        activityid,
        any_value(activityname) AS activityname,
        any_value(activitytype) AS activitytype,
        any_value(starttimelocal) AS starttimelocal,
        any_value(data_type) AS data_type,
        max(_ingested_at) AS _ingested_at,

        -- Build ARRAY from flat columns when populated (recent format),
        -- fall back to hr_zones_data JSON blob (legacy format).
        CASE
            WHEN countif(zonenumber IS NOT NULL) > 0
                THEN array_agg(
                    STRUCT(
                        zonenumber AS zone_number,
                        secsinzone AS secs_in_zone,
                        zonelowboundary AS zone_low_boundary
                    )
                    ORDER BY zonenumber
                )
            ELSE any_value(
                array(
                    SELECT
                        STRUCT(
                            cast(json_value(item, '$.zoneNumber') AS INT64) AS zone_number,
                            cast(json_value(item, '$.secsInZone') AS FLOAT64) AS secs_in_zone,
                            cast(json_value(item, '$.zoneLowBoundary') AS INT64) AS zone_low_boundary
                        )
                    FROM unnest(json_query_array(hr_zones_data)) AS item
                )
            )
        END AS hr_zones

    FROM zones
    GROUP BY activityid
)

SELECT * FROM aggregated
