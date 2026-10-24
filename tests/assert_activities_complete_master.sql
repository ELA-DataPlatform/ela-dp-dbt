{{ config(tags=['garmin', 'activities']) }}

-- Missing optional detail must never remove an activity or multiply its parent.
SELECT
    COALESCE(a.activityid, m.activity_id) AS activity_id,
    'source_master_mismatch' AS issue
FROM {{ ref('dlk_garmin_svc__activities') }} AS a
FULL OUTER JOIN {{ ref('hub_activities_svc__master_activities') }} AS m
    ON a.activityid = m.activity_id
WHERE
    a.activityid IS NULL OR m.activity_id IS NULL
    OR a.activity_type.type_key IS DISTINCT FROM m.sport.type_key
    OR a.starttimegmt IS DISTINCT FROM m.timing.start_at_utc
    OR DATETIME(a.starttimelocal) IS DISTINCT FROM m.timing.start_at_local

UNION ALL

SELECT activity_id, 'duplicate_master_activity' AS issue
FROM {{ ref('hub_activities_svc__master_activities') }}
GROUP BY activity_id
HAVING COUNT(*) != 1
