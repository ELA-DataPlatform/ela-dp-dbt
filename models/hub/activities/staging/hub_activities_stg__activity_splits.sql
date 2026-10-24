{{ config(materialized='view') }}

WITH laps AS (
    SELECT
        activity_id,
        ARRAY_AGG(
            (SELECT AS STRUCT p.* EXCEPT (activity_id))
            ORDER BY source_index
        ) AS items
    FROM {{ ref('hub_activities_stg__activity_laps') }} AS p
    GROUP BY activity_id
),

typed_splits AS (
    SELECT
        activity_id,
        ARRAY_AGG(
            (SELECT AS STRUCT p.* EXCEPT (activity_id))
            ORDER BY source_index
        ) AS items
    FROM {{ ref('hub_activities_stg__activity_typed_splits') }} AS p
    GROUP BY activity_id
),

split_summaries AS (
    SELECT
        activity_id,
        ARRAY_AGG(
            (SELECT AS STRUCT p.* EXCEPT (activity_id))
            ORDER BY source_index
        ) AS items
    FROM {{ ref('hub_activities_stg__activity_split_summaries') }} AS p
    GROUP BY activity_id
),

prepared AS (
    SELECT
        s.activityid AS activity_id,
        s._ingested_at,
        COALESCE(l.items, []) AS laps,
        COALESCE(t.items, []) AS typed_splits,
        COALESCE(c.items, []) AS split_summaries
    FROM {{ ref('dlk_garmin_svc__activity_splits') }} AS s
    LEFT JOIN laps AS l ON s.activityid = l.activity_id
    LEFT JOIN typed_splits AS t ON s.activityid = t.activity_id
    LEFT JOIN split_summaries AS c ON s.activityid = c.activity_id
)

SELECT
    p.*,
    'garmin' AS source_system,
    'activity_splits' AS source_table,
    CAST(activity_id AS STRING) AS source_record_key,
    CASE
        WHEN
            activity_id IS NULL OR EXISTS (
                SELECT 1 FROM UNNEST(p.laps) AS v
                WHERE v.quality_status = 'invalid') OR EXISTS (SELECT 1 FROM UNNEST(p.typed_splits) AS v
            WHERE v.quality_status = 'invalid') OR EXISTS (
                SELECT 1 FROM UNNEST(p.split_summaries) AS v
                WHERE v.quality_status = 'invalid'
            )
            THEN 'invalid'
        WHEN
            ARRAY_LENGTH(laps) + ARRAY_LENGTH(typed_splits) + ARRAY_LENGTH(split_summaries) = 0
            OR EXISTS (
                SELECT 1 FROM UNNEST(p.laps) AS v
                WHERE v.quality_status = 'partial'
            ) THEN 'partial'
        ELSE 'valid'
    END AS quality_status,
    ARRAY_CONCAT(
        IF(activity_id IS NULL, ['missing_activity_id'], ARRAY<STRING>[]),
        IF(
            EXISTS (
                SELECT 1 FROM UNNEST(p.laps) AS v
                WHERE v.quality_status = 'partial'
            ),
            ['partial_laps'], ARRAY<STRING>[]
        ),
        IF(
            EXISTS (
                SELECT 1 FROM UNNEST(p.laps) AS v
                WHERE v.quality_status = 'invalid'
            ), ['invalid_laps'
            ], ARRAY<STRING>[]
        ),
        IF(
            EXISTS (
                SELECT 1 FROM UNNEST(p.typed_splits) AS v
                WHERE v.quality_status = 'invalid'
            ), ['invalid_typed_splits'
            ], ARRAY<STRING>[]
        ),
        IF(
            EXISTS (
                SELECT 1 FROM UNNEST(p.split_summaries) AS v
                WHERE v.quality_status = 'invalid'
            ), ['invalid_split_summaries'
            ], ARRAY<STRING>[]
        ),
        IF(
            ARRAY_LENGTH(laps) + ARRAY_LENGTH(typed_splits) + ARRAY_LENGTH(split_summaries) = 0,
            ['no_splits_in_response'], ARRAY<STRING>[]
        )
    ) AS quality_issues
FROM prepared AS p
