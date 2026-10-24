{{ config(materialized='view') }}

WITH activity_ids AS (
    SELECT activityid FROM {{ ref('dlk_garmin_svc__activities') }}
    UNION DISTINCT
    SELECT activityid FROM {{ ref('dlk_garmin_svc__activity_hr_zones') }}
),

prepared AS (
    SELECT
        ids.activityid AS activity_id,
        ARRAY(
            SELECT AS STRUCT
                source_index,
                SAFE_CAST({{ garmin_activity_number('z.zone_number', 1, none) }} AS INT64) AS zone_number,
                {{ garmin_activity_number('z.secs_in_zone', 0, none) }} AS duration_s,
                SAFE_CAST({{ garmin_activity_number('z.zone_low_boundary', 0, none) }} AS INT64) AS low_boundary_bpm,
                z AS source_values,
                IF(
                    z.zone_number IS NULL OR z.secs_in_zone IS NULL, 'partial',
                    IF(
                        {{ garmin_activity_number('z.zone_number', 1, none) }} IS NULL
                        OR {{ garmin_activity_number('z.secs_in_zone', 0, none) }} IS NULL
                        OR (
                            z.zone_low_boundary IS NOT NULL
                            AND {{ garmin_activity_number('z.zone_low_boundary', 0, none) }} IS NULL
                        ),
                        'invalid', 'valid'
                    )
                ) AS quality_status,
                ARRAY_CONCAT(
                    IF(z.zone_number IS NULL, ['missing_zone_number'], ARRAY<STRING>[]),
                    IF(z.secs_in_zone IS NULL, ['missing_duration_s'], ARRAY<STRING>[]),
                    IF(
                        z.zone_number IS NOT NULL AND {{ garmin_activity_number('z.zone_number', 1, none) }} IS NULL,
                        ['invalid_zone_number'], ARRAY<STRING>[]
                    ),
                    IF(
                        z.secs_in_zone IS NOT NULL AND {{ garmin_activity_number('z.secs_in_zone', 0, none) }} IS NULL,
                        ['invalid_duration_s'], ARRAY<STRING>[]
                    ),
                    IF(
                        z.zone_low_boundary IS NOT NULL
                        AND {{ garmin_activity_number('z.zone_low_boundary', 0, none) }} IS NULL,
                        ['invalid_low_boundary_bpm'], ARRAY<STRING>[]
                    )
                ) AS quality_issues
            FROM UNNEST(h.hr_zones) AS z WITH OFFSET AS source_index
            ORDER BY z.zone_number IS NULL, z.zone_number, source_index
        ) AS heart_rate,
        ARRAY(
            SELECT AS STRUCT
                z.zone_number,
                {{ garmin_activity_number('z.duration_s', 0, none) }} AS duration_s,
                z.duration_s AS source_value,
                IF({{ garmin_activity_number('z.duration_s', 0, none) }} IS NULL, 'invalid', 'valid') AS quality_status,
                IF(
                    {{ garmin_activity_number('z.duration_s', 0, none) }} IS NULL,
                    ['invalid_duration_s'],
                    ARRAY<STRING>[]
                )
                    AS quality_issues
            FROM
                UNNEST([
                    STRUCT(1 AS zone_number, a.powertimeinzone_1 AS duration_s),
                    STRUCT(2 AS zone_number, a.powertimeinzone_2 AS duration_s),
                    STRUCT(3 AS zone_number, a.powertimeinzone_3 AS duration_s),
                    STRUCT(4 AS zone_number, a.powertimeinzone_4 AS duration_s),
                    STRUCT(5 AS zone_number, a.powertimeinzone_5 AS duration_s)
                ]) AS z
            WHERE z.duration_s IS NOT NULL
            ORDER BY z.zone_number
        ) AS power,
        h.activityid IS NOT NULL AS heart_rate_response_present,
        h._ingested_at AS heart_rate_ingested_at,
        a._ingested_at AS summary_ingested_at,
        GREATEST(COALESCE(a._ingested_at, h._ingested_at), COALESCE(h._ingested_at, a._ingested_at)) AS _ingested_at
    FROM activity_ids AS ids
    LEFT JOIN {{ ref('dlk_garmin_svc__activities') }} AS a ON ids.activityid = a.activityid
    LEFT JOIN {{ ref('dlk_garmin_svc__activity_hr_zones') }} AS h ON ids.activityid = h.activityid
)

SELECT
    p.*,
    'garmin' AS source_system,
    'activity_hr_zones+activities' AS source_table,
    CAST(activity_id AS STRING) AS source_record_key,
    CASE
        WHEN
            activity_id IS NULL
            OR EXISTS (
                SELECT 1 FROM UNNEST(heart_rate) AS z
                WHERE z.quality_status = 'invalid'
            )
            OR EXISTS (
                SELECT 1 FROM UNNEST(power) AS z
                WHERE z.quality_status = 'invalid'
            )
            OR EXISTS (
                SELECT 1 FROM UNNEST(heart_rate) AS z
                GROUP BY z.zone_number
                HAVING COUNT(*) > 1
            )
            THEN 'invalid'
        WHEN
            ARRAY_LENGTH(heart_rate) + ARRAY_LENGTH(power) = 0
            OR EXISTS (
                SELECT 1 FROM UNNEST(heart_rate) AS z
                WHERE z.quality_status = 'partial'
            ) THEN 'partial'
        ELSE 'valid'
    END AS quality_status,
    ARRAY_CONCAT(
        IF(activity_id IS NULL, ['missing_activity_id'], ARRAY<STRING>[]),
        IF(
            EXISTS (
                SELECT 1 FROM UNNEST(heart_rate) AS z
                WHERE z.quality_status = 'invalid'
            ),
            ['invalid_heart_rate_zones'], ARRAY<STRING>[]
        ),
        IF(
            EXISTS (
                SELECT 1 FROM UNNEST(power) AS z
                WHERE z.quality_status = 'invalid'
            ),
            ['invalid_power_zones'], ARRAY<STRING>[]
        ),
        IF(
            EXISTS (
                SELECT 1 FROM UNNEST(heart_rate) AS z
                GROUP BY z.zone_number
                HAVING COUNT(*) > 1
            ),
            ['duplicate_heart_rate_zone'], ARRAY<STRING>[]
        ),
        IF(
            EXISTS (
                SELECT 1 FROM UNNEST(heart_rate) AS z
                WHERE z.quality_status = 'partial'
            ),
            ['partial_heart_rate_zones'], ARRAY<STRING>[]
        ),
        IF(ARRAY_LENGTH(heart_rate) + ARRAY_LENGTH(power) = 0, ['no_zones_in_source'], ARRAY<STRING>[])
    ) AS quality_issues
FROM prepared AS p
