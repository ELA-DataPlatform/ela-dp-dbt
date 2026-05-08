{{
    config(
        materialized='view',
        tags=['garmin'],
    )
}}

WITH source AS (
    SELECT
        activityId,
        data_type,
        detailsAvailable,
        totalMetricsCount,
        metricsCount,
        measurementCount,
        heartRateDTOs,
        pendingData,
        _ingested_at,

        -- Parse metricDescriptors → ARRAY<STRUCT>
        ARRAY(
            SELECT STRUCT(
                CAST(JSON_VALUE(item, '$.metricsIndex') AS INT64) AS metrics_index,
                JSON_VALUE(item, '$.key')                         AS key,
                CAST(JSON_VALUE(item, '$.unit.id') AS INT64)      AS unit_id,
                JSON_VALUE(item, '$.unit.key')                    AS unit_key,
                CAST(JSON_VALUE(item, '$.unit.factor') AS FLOAT64) AS unit_factor
            )
            FROM UNNEST(JSON_QUERY_ARRAY(metricDescriptors)) AS item
        ) AS metric_descriptors,

        -- Parse activityDetailMetrics → ARRAY<STRUCT<metrics ARRAY<FLOAT64>>>
        ARRAY(
            SELECT STRUCT(
                ARRAY(
                    SELECT CAST(v AS FLOAT64)
                    FROM UNNEST(JSON_VALUE_ARRAY(item, '$.metrics')) AS v
                    WHERE v IS NOT NULL
                ) AS metrics
            )
            FROM UNNEST(JSON_QUERY_ARRAY(activityDetailMetrics)) AS item
        ) AS activity_detail_metrics,

        -- Parse geoPolylineDTO → STRUCT
        IF(
            geoPolylineDTO IS NULL,
            NULL,
            STRUCT(
                STRUCT(
                    CAST(JSON_VALUE(geoPolylineDTO, '$.startPoint.lat') AS FLOAT64)                      AS lat,
                    CAST(JSON_VALUE(geoPolylineDTO, '$.startPoint.lon') AS FLOAT64)                      AS lon,
                    CAST(JSON_VALUE(geoPolylineDTO, '$.startPoint.altitude') AS FLOAT64)                 AS altitude,
                    CAST(JSON_VALUE(geoPolylineDTO, '$.startPoint.time') AS INT64)                       AS time,
                    CAST(JSON_VALUE(geoPolylineDTO, '$.startPoint.timerStart') AS BOOL)                  AS timer_start,
                    CAST(JSON_VALUE(geoPolylineDTO, '$.startPoint.timerStop') AS BOOL)                   AS timer_stop,
                    CAST(JSON_VALUE(geoPolylineDTO, '$.startPoint.distanceFromPreviousPoint') AS FLOAT64) AS distance_from_previous_point,
                    CAST(JSON_VALUE(geoPolylineDTO, '$.startPoint.distanceInMeters') AS FLOAT64)         AS distance_in_meters,
                    CAST(JSON_VALUE(geoPolylineDTO, '$.startPoint.speed') AS FLOAT64)                    AS speed,
                    CAST(JSON_VALUE(geoPolylineDTO, '$.startPoint.cumulativeAscent') AS FLOAT64)         AS cumulative_ascent,
                    CAST(JSON_VALUE(geoPolylineDTO, '$.startPoint.cumulativeDescent') AS FLOAT64)        AS cumulative_descent,
                    CAST(JSON_VALUE(geoPolylineDTO, '$.startPoint.extendedCoordinate') AS BOOL)          AS extended_coordinate,
                    CAST(JSON_VALUE(geoPolylineDTO, '$.startPoint.valid') AS BOOL)                       AS valid
                ) AS start_point,
                STRUCT(
                    CAST(JSON_VALUE(geoPolylineDTO, '$.endPoint.lat') AS FLOAT64)                      AS lat,
                    CAST(JSON_VALUE(geoPolylineDTO, '$.endPoint.lon') AS FLOAT64)                      AS lon,
                    CAST(JSON_VALUE(geoPolylineDTO, '$.endPoint.altitude') AS FLOAT64)                 AS altitude,
                    CAST(JSON_VALUE(geoPolylineDTO, '$.endPoint.time') AS INT64)                       AS time,
                    CAST(JSON_VALUE(geoPolylineDTO, '$.endPoint.timerStart') AS BOOL)                  AS timer_start,
                    CAST(JSON_VALUE(geoPolylineDTO, '$.endPoint.timerStop') AS BOOL)                   AS timer_stop,
                    CAST(JSON_VALUE(geoPolylineDTO, '$.endPoint.distanceFromPreviousPoint') AS FLOAT64) AS distance_from_previous_point,
                    CAST(JSON_VALUE(geoPolylineDTO, '$.endPoint.distanceInMeters') AS FLOAT64)         AS distance_in_meters,
                    CAST(JSON_VALUE(geoPolylineDTO, '$.endPoint.speed') AS FLOAT64)                    AS speed,
                    CAST(JSON_VALUE(geoPolylineDTO, '$.endPoint.cumulativeAscent') AS FLOAT64)         AS cumulative_ascent,
                    CAST(JSON_VALUE(geoPolylineDTO, '$.endPoint.cumulativeDescent') AS FLOAT64)        AS cumulative_descent,
                    CAST(JSON_VALUE(geoPolylineDTO, '$.endPoint.extendedCoordinate') AS BOOL)          AS extended_coordinate,
                    CAST(JSON_VALUE(geoPolylineDTO, '$.endPoint.valid') AS BOOL)                       AS valid
                ) AS end_point,
                CAST(JSON_VALUE(geoPolylineDTO, '$.minLat') AS FLOAT64) AS min_lat,
                CAST(JSON_VALUE(geoPolylineDTO, '$.maxLat') AS FLOAT64) AS max_lat,
                CAST(JSON_VALUE(geoPolylineDTO, '$.minLon') AS FLOAT64) AS min_lon,
                CAST(JSON_VALUE(geoPolylineDTO, '$.maxLon') AS FLOAT64) AS max_lon,
                ARRAY(
                    SELECT STRUCT(
                        CAST(JSON_VALUE(pt, '$.lat') AS FLOAT64)                      AS lat,
                        CAST(JSON_VALUE(pt, '$.lon') AS FLOAT64)                      AS lon,
                        CAST(JSON_VALUE(pt, '$.altitude') AS FLOAT64)                 AS altitude,
                        CAST(JSON_VALUE(pt, '$.time') AS INT64)                       AS time,
                        CAST(JSON_VALUE(pt, '$.timerStart') AS BOOL)                  AS timer_start,
                        CAST(JSON_VALUE(pt, '$.timerStop') AS BOOL)                   AS timer_stop,
                        CAST(JSON_VALUE(pt, '$.distanceFromPreviousPoint') AS FLOAT64) AS distance_from_previous_point,
                        CAST(JSON_VALUE(pt, '$.distanceInMeters') AS FLOAT64)         AS distance_in_meters,
                        CAST(JSON_VALUE(pt, '$.speed') AS FLOAT64)                    AS speed,
                        CAST(JSON_VALUE(pt, '$.cumulativeAscent') AS FLOAT64)         AS cumulative_ascent,
                        CAST(JSON_VALUE(pt, '$.cumulativeDescent') AS FLOAT64)        AS cumulative_descent,
                        CAST(JSON_VALUE(pt, '$.extendedCoordinate') AS BOOL)          AS extended_coordinate,
                        CAST(JSON_VALUE(pt, '$.valid') AS BOOL)                       AS valid
                    )
                    FROM UNNEST(JSON_QUERY_ARRAY(geoPolylineDTO, '$.polyline')) AS pt
                ) AS polyline
            )
        ) AS geo_polyline

    FROM {{ source('garmin', 'normalized_activity_details') }}
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY activityId
            ORDER BY _ingested_at DESC
        ) AS _row_number
    FROM source
)

SELECT * EXCEPT (_row_number)
FROM deduplicated
WHERE _row_number = 1
