{{ config(materialized='view') }}

WITH samples AS (
    SELECT
        activity_id,
        ARRAY_AGG(STRUCT(
            sample_key,
            sample_index,
            timestamp_gmt,
            elapsed_s,
            duration_s,
            moving_duration_s,
            cum_distance_m,
            speed_m_per_s,
            grade_adj_speed_m_per_s,
            heart_rate_bpm,
            body_battery,
            respiration_rpm,
            performance_condition,
            cadence_spm,
            fractional_cadence,
            run_cadence_spm,
            ground_contact_ms,
            stride_length_m,
            vertical_oscillation_mm,
            vertical_ratio,
            vertical_speed_m_per_s,
            latitude,
            longitude,
            elevation_m,
            corrected_elevation_m,
            power_w,
            accumulated_power_w,
            air_temp_celsius,
            stamina_available,
            stamina_potential,
            quality_status,
            quality_issues,
            source_values
        )
        ORDER BY timestamp_gmt IS NULL, timestamp_gmt, sample_index) AS timeseries,
        COUNTIF(quality_status = 'invalid') AS invalid_sample_count,
        COUNTIF(quality_status = 'partial') AS partial_sample_count
    FROM {{ ref('hub_activities_stg__activity_samples') }}
    GROUP BY activity_id
),

route_quality AS (
    SELECT
        d.activityid AS activity_id,
        COUNTIF(
            pt.valid IS FALSE
            OR (pt.lat IS NOT NULL AND {{ garmin_activity_number('pt.lat', -90, 90) }} IS NULL)
            OR (pt.lon IS NOT NULL AND {{ garmin_activity_number('pt.lon', -180, 180) }} IS NULL)
            OR (pt.time_ms IS NOT NULL AND SAFE.TIMESTAMP_MILLIS(pt.time_ms) IS NULL)
            OR pt.speed < 0 OR pt.distance_in_meters < 0 OR pt.distance_from_previous_point < 0
            OR pt.cumulative_ascent < 0 OR pt.cumulative_descent < 0
        ) AS invalid_route_point_count
    FROM {{ ref('dlk_garmin_svc__activity_details') }} AS d
    LEFT JOIN UNNEST(ARRAY_CONCAT(
        [d.geo_polyline.start_point, d.geo_polyline.end_point],
        COALESCE(d.geo_polyline.polyline, [])
    )) AS pt ON TRUE
    GROUP BY d.activityid
)

SELECT
    d.activityid AS activity_id,
    COALESCE(s.timeseries, []) AS timeseries,
    IF(d.geo_polyline IS NULL, NULL, STRUCT(
        (SELECT STRUCT(
            {{ garmin_activity_number('pt.lat', -90, 90) }} AS lat,
            {{ garmin_activity_number('pt.lon', -180, 180) }} AS lon,
            {{ garmin_activity_number('pt.altitude', none, none) }} AS altitude,
            pt.time_ms AS time_ms,
            pt.timer_start AS timer_start,
            pt.timer_stop AS timer_stop,
            {{ garmin_activity_number('pt.distance_from_previous_point', 0, none) }} AS distance_from_previous_point,
            {{ garmin_activity_number('pt.distance_in_meters', 0, none) }} AS distance_in_meters,
            {{ garmin_activity_number('pt.speed', 0, none) }} AS speed,
            {{ garmin_activity_number('pt.cumulative_ascent', 0, none) }} AS cumulative_ascent,
            {{ garmin_activity_number('pt.cumulative_descent', 0, none) }} AS cumulative_descent,
            pt.extended_coordinate AS extended_coordinate,
            pt.valid AS valid
        ) FROM UNNEST([d.geo_polyline.start_point]) AS pt) AS start_point,
        (SELECT STRUCT(
            {{ garmin_activity_number('pt.lat', -90, 90) }} AS lat,
            {{ garmin_activity_number('pt.lon', -180, 180) }} AS lon,
            {{ garmin_activity_number('pt.altitude', none, none) }} AS altitude,
            pt.time_ms AS time_ms,
            pt.timer_start AS timer_start,
            pt.timer_stop AS timer_stop,
            {{ garmin_activity_number('pt.distance_from_previous_point', 0, none) }} AS distance_from_previous_point,
            {{ garmin_activity_number('pt.distance_in_meters', 0, none) }} AS distance_in_meters,
            {{ garmin_activity_number('pt.speed', 0, none) }} AS speed,
            {{ garmin_activity_number('pt.cumulative_ascent', 0, none) }} AS cumulative_ascent,
            {{ garmin_activity_number('pt.cumulative_descent', 0, none) }} AS cumulative_descent,
            pt.extended_coordinate AS extended_coordinate,
            pt.valid AS valid
        ) FROM UNNEST([d.geo_polyline.end_point]) AS pt) AS end_point,
        {{ garmin_activity_number('d.geo_polyline.min_lat', -90, 90) }} AS min_lat,
        {{ garmin_activity_number('d.geo_polyline.max_lat', -90, 90) }} AS max_lat,
        {{ garmin_activity_number('d.geo_polyline.min_lon', -180, 180) }} AS min_lon,
        {{ garmin_activity_number('d.geo_polyline.max_lon', -180, 180) }} AS max_lon,
        ARRAY(SELECT STRUCT(
            {{ garmin_activity_number('pt.lat', -90, 90) }} AS lat,
            {{ garmin_activity_number('pt.lon', -180, 180) }} AS lon,
            {{ garmin_activity_number('pt.altitude', none, none) }} AS altitude,
            pt.time_ms AS time_ms,
            pt.timer_start AS timer_start,
            pt.timer_stop AS timer_stop,
            {{ garmin_activity_number('pt.distance_from_previous_point', 0, none) }} AS distance_from_previous_point,
            {{ garmin_activity_number('pt.distance_in_meters', 0, none) }} AS distance_in_meters,
            {{ garmin_activity_number('pt.speed', 0, none) }} AS speed,
            {{ garmin_activity_number('pt.cumulative_ascent', 0, none) }} AS cumulative_ascent,
            {{ garmin_activity_number('pt.cumulative_descent', 0, none) }} AS cumulative_descent,
            pt.extended_coordinate AS extended_coordinate,
            pt.valid AS valid
        ) FROM UNNEST(d.geo_polyline.polyline) AS pt WITH OFFSET AS point_index
        ORDER BY point_index) AS polyline
    )) AS route,
    COALESCE(rq.invalid_route_point_count, 0) AS invalid_route_point_count,
    d.metric_descriptors,
    d.detailsavailable AS source_details_available,
    d.totalmetricscount AS source_total_metrics_count,
    d.metricscount AS source_metrics_count,
    d.measurementcount AS source_measurement_count,
    COALESCE(s.invalid_sample_count, 0) AS invalid_sample_count,
    COALESCE(s.partial_sample_count, 0) AS partial_sample_count,
    STRUCT(d.geo_polyline AS route, d.pendingdata AS pending_data, d.heartratedtos AS heart_rate_dtos) AS source_values,
    'garmin' AS source_system,
    'activity_details' AS source_table,
    CAST(d.activityid AS STRING) AS source_record_key,
    d._ingested_at,
    CASE
        WHEN
            d.activityid IS NULL OR COALESCE(s.invalid_sample_count, 0) > 0
            OR COALESCE(rq.invalid_route_point_count, 0) > 0 THEN 'invalid'
        WHEN
            ARRAY_LENGTH(s.timeseries) IS NULL OR ARRAY_LENGTH(s.timeseries) = 0
            OR COALESCE(s.partial_sample_count, 0) > 0 THEN 'partial'
        ELSE 'valid'
    END AS quality_status,
    ARRAY_CONCAT(
        IF(d.activityid IS NULL, ['missing_activity_id'], ARRAY<STRING>[]),
        IF(COALESCE(rq.invalid_route_point_count, 0) > 0, ['invalid_route_points'], ARRAY<STRING>[]),
        IF(COALESCE(s.invalid_sample_count, 0) > 0, ['invalid_samples'], ARRAY<STRING>[]),
        IF(COALESCE(s.partial_sample_count, 0) > 0, ['partial_samples'], ARRAY<STRING>[]),
        IF(COALESCE(ARRAY_LENGTH(s.timeseries), 0) = 0, ['no_samples_in_response'], ARRAY<STRING>[])
    ) AS quality_issues
FROM {{ ref('dlk_garmin_svc__activity_details') }} AS d
LEFT JOIN samples AS s ON d.activityid = s.activity_id
LEFT JOIN route_quality AS rq ON d.activityid = rq.activity_id
