{{
    config(
        materialized='view',
        tags=['garmin'],
    )
}}

WITH source AS (
    SELECT
        * EXCEPT (generic, heataltitudeacclimation),

        -- Parse generic (JSON object) → VO2 max fields
        cast(json_value(generic, '$.vo2MaxPreciseValue') AS float64) AS vo2_max_precise,
        cast(json_value(generic, '$.vo2MaxValue') AS float64) AS vo2_max,
        cast(json_value(generic, '$.maxMetCategory') AS int64) AS max_met_category,
        cast(json_value(heataltitudeacclimation, '$.altitudeAcclimation') AS float64) AS altitude_acclimation,

        -- Parse heatAltitudeAcclimation (JSON object) → acclimation fields
        cast(json_value(heataltitudeacclimation, '$.previousAltitudeAcclimation') AS float64)
            AS previous_altitude_acclimation,
        cast(json_value(heataltitudeacclimation, '$.heatAcclimationPercentage') AS float64) AS heat_acclimation_pct,
        cast(json_value(heataltitudeacclimation, '$.previousHeatAcclimationPercentage') AS float64)
            AS previous_heat_acclimation_pct,
        cast(json_value(heataltitudeacclimation, '$.currentAltitude') AS float64) AS current_altitude,
        cast(json_value(heataltitudeacclimation, '$.acclimationPercentage') AS float64) AS acclimation_pct,
        json_value(generic, '$.calendarDate') AS vo2_max_calendar_date,
        json_value(heataltitudeacclimation, '$.calendarDate') AS acclimation_calendar_date,
        json_value(heataltitudeacclimation, '$.altitudeAcclimationDate') AS altitude_acclimation_date,
        json_value(heataltitudeacclimation, '$.heatAcclimationDate') AS heat_acclimation_date

    FROM {{ source('garmin', 'normalized_max_metrics') }}
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
