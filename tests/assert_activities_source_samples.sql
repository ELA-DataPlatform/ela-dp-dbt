{{ config(tags=['garmin', 'activities']) }}

-- Independent check against canonical raw JSON: neither samples nor NULL slots
-- may disappear during lake parsing. Unknown descriptors remain as source slots.
WITH raw AS (
    SELECT activityid, activitydetailmetrics
    FROM {{ source('garmin', 'normalized_activity_details') }} AS r
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY activityid
        ORDER BY _ingested_at DESC, TO_JSON_STRING(r) ASC
    ) = 1
),

raw_counts AS (
    SELECT
        r.activityid,
        ARRAY_LENGTH(JSON_QUERY_ARRAY(r.activitydetailmetrics)) AS sample_count,
        (
            SELECT SUM(ARRAY_LENGTH(JSON_QUERY_ARRAY(p, '$.metrics')))
            FROM UNNEST(JSON_QUERY_ARRAY(r.activitydetailmetrics)) AS p
        ) AS slot_count
    FROM raw AS r
),

parsed_counts AS (
    SELECT
        activityid,
        ARRAY_LENGTH(indexed_metric_samples) AS sample_count,
        (
            SELECT SUM(ARRAY_LENGTH(p.metric_values))
            FROM UNNEST(indexed_metric_samples) AS p
        ) AS slot_count
    FROM {{ ref('dlk_garmin_svc__activity_details') }}
),

hub_counts AS (
    SELECT activity_id, COUNT(*) AS sample_count
    FROM {{ ref('hub_activities_stg__activity_samples') }}
    GROUP BY activity_id
)

SELECT r.activityid
FROM raw_counts AS r
LEFT JOIN parsed_counts AS p ON r.activityid = p.activityid
LEFT JOIN hub_counts AS h ON r.activityid = h.activity_id
WHERE
    COALESCE(r.sample_count, 0) != COALESCE(p.sample_count, 0)
    OR COALESCE(r.slot_count, 0) != COALESCE(p.slot_count, 0)
    OR COALESCE(p.sample_count, 0) != COALESCE(h.sample_count, 0)

UNION ALL

-- Check values and positions, not only array lengths. A null before the HR slot
-- must not shift HR into the speed slot or silently disappear.
SELECT r.activityid
FROM raw AS r
INNER JOIN {{ ref('dlk_garmin_svc__activity_details') }} AS d ON r.activityid = d.activityid
WHERE EXISTS (
    SELECT 1
    FROM UNNEST(JSON_QUERY_ARRAY(r.activitydetailmetrics)) AS sample WITH OFFSET AS sample_index,
        UNNEST(JSON_QUERY_ARRAY(sample, '$.metrics')) AS value WITH OFFSET AS metric_index
    WHERE
        value IS DISTINCT FROM d.indexed_metric_samples[SAFE_OFFSET(sample_index)]
            .metric_values[SAFE_OFFSET(metric_index)].source_text
        OR SAFE_CAST(JSON_VALUE(value) AS FLOAT64) IS DISTINCT FROM d.indexed_metric_samples[SAFE_OFFSET(sample_index)]
            .metric_values[SAFE_OFFSET(metric_index)].metric_value
)
