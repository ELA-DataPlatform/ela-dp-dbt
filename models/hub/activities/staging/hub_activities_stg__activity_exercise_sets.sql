{{ config(materialized='view') }}

WITH prepared AS (
    SELECT
        s.activityid AS activity_id,
        ARRAY(
            SELECT AS STRUCT
                CONCAT(CAST(s.activityid AS STRING), ':set:', CAST(set_index AS STRING)) AS set_key,
                set_index,
                p.message_index,
                p.set_type,
                {{ garmin_health_timestamp('p.start_time') }} AS start_at_utc,
                {{ garmin_activity_number('p.duration', 0, none) }} AS duration_s,
                SAFE_CAST({{ garmin_activity_number('p.repetition_count', 0, none) }} AS INT64) AS repetition_count,
                {{ garmin_activity_number('p.weight_g', 0, none) }} / 1000.0 AS weight_kg,
                ARRAY(
                    SELECT AS STRUCT
                        candidate_index,
                        e.category,
                        {{ garmin_activity_number('e.probability', 0, 1) }} AS probability,
                        e.probability AS source_probability,
                        IF(
                            e.probability IS NOT NULL AND {{ garmin_activity_number('e.probability', 0, 1) }} IS NULL,
                            'invalid', 'valid'
                        ) AS quality_status
                    FROM UNNEST(p.exercises) AS e WITH OFFSET AS candidate_index
                    ORDER BY candidate_index
                ) AS exercises,
                p AS source_values,
                CASE
                    WHEN
                        ARRAY_LENGTH(ARRAY_CONCAT(
                            IF(
                                p.duration IS NOT NULL AND {{ garmin_activity_number('p.duration', 0, none) }} IS NULL,
                                ['invalid_duration_s'],
                                ARRAY<STRING>[]
                            ),
                            IF(
                                p.repetition_count IS NOT NULL
                                AND {{ garmin_activity_number('p.repetition_count', 0, none) }} IS NULL,
                                ['invalid_repetition_count'],
                                ARRAY<STRING>[]
                            ),
                            IF(
                                p.weight_g IS NOT NULL AND {{ garmin_activity_number('p.weight_g', 0, none) }} IS NULL,
                                ['invalid_weight_kg'],
                                ARRAY<STRING>[]
                            )
                        )) > 0
                        OR (p.start_time IS NOT NULL AND {{ garmin_health_timestamp('p.start_time') }} IS NULL)
                        OR EXISTS (
                            SELECT 1 FROM UNNEST(p.exercises) AS e
                            WHERE
                                e.probability IS NOT NULL
                                AND {{ garmin_activity_number('e.probability', 0, 1) }} IS NULL
                        )
                        THEN 'invalid'
                    WHEN p.set_type IS NULL OR p.duration IS NULL THEN 'partial'
                    ELSE 'valid'
                END AS quality_status,
                ARRAY_CONCAT(
                    ARRAY_CONCAT(
                        IF(
                            p.duration IS NOT NULL AND {{ garmin_activity_number('p.duration', 0, none) }} IS NULL,
                            ['invalid_duration_s'],
                            ARRAY<STRING>[]
                        ),
                        IF(
                            p.repetition_count IS NOT NULL
                            AND {{ garmin_activity_number('p.repetition_count', 0, none) }} IS NULL,
                            ['invalid_repetition_count'],
                            ARRAY<STRING>[]
                        ),
                        IF(
                            p.weight_g IS NOT NULL AND {{ garmin_activity_number('p.weight_g', 0, none) }} IS NULL,
                            ['invalid_weight_kg'],
                            ARRAY<STRING>[]
                        )
                    ),
                    IF(
                        p.start_time IS NOT NULL AND {{ garmin_health_timestamp('p.start_time') }} IS NULL,
                        ['invalid_start_at_utc'], ARRAY<STRING>[]
                    ),
                    IF(p.set_type IS NULL, ['missing_set_type'], ARRAY<STRING>[]),
                    IF(p.duration IS NULL, ['missing_duration_s'], ARRAY<STRING>[]),
                    IF(EXISTS (
                        SELECT 1 FROM UNNEST(p.exercises) AS e
                        WHERE e.probability IS NOT NULL AND {{ garmin_activity_number('e.probability', 0, 1) }} IS NULL
                    ),
                    ['invalid_exercise_probability'], ARRAY<STRING>[])
                ) AS quality_issues
            FROM UNNEST(s.exercise_sets) AS p WITH OFFSET AS set_index
            ORDER BY set_index
        ) AS exercise_sets,
        s._ingested_at
    FROM {{ ref('dlk_garmin_svc__activity_exercise_sets') }} AS s
)

SELECT
    p.*,
    'garmin' AS source_system,
    'activity_exercise_sets' AS source_table,
    CAST(activity_id AS STRING) AS source_record_key,
    CASE
        WHEN
            activity_id IS NULL
            OR EXISTS (
                SELECT 1 FROM UNNEST(exercise_sets) AS e
                WHERE e.quality_status = 'invalid'
            ) THEN 'invalid'
        WHEN
            ARRAY_LENGTH(exercise_sets) = 0
            OR EXISTS (
                SELECT 1 FROM UNNEST(exercise_sets) AS e
                WHERE e.quality_status = 'partial'
            ) THEN 'partial'
        ELSE 'valid'
    END AS quality_status,
    ARRAY_CONCAT(
        IF(activity_id IS NULL, ['missing_activity_id'], ARRAY<STRING>[]),
        IF(ARRAY_LENGTH(exercise_sets) = 0, ['no_sets_in_response'], ARRAY<STRING>[]),
        IF(
            EXISTS (
                SELECT 1 FROM UNNEST(exercise_sets) AS e
                WHERE e.quality_status = 'invalid'
            ),
            ['invalid_sets'], ARRAY<STRING>[]
        ),
        IF(
            EXISTS (
                SELECT 1 FROM UNNEST(exercise_sets) AS e
                WHERE e.quality_status = 'partial'
            ),
            ['partial_sets'], ARRAY<STRING>[]
        )
    ) AS quality_issues
FROM prepared AS p
