{{ config(tags=['garmin', 'activities']) }}

-- The descriptive Health association is explicitly single-account. Fail rather
-- than silently attach another account's daily health to an activity.
SELECT 'multiple_activity_owners' AS issue
FROM (
    SELECT COUNT(DISTINCT identity.source_user_id) AS owner_count
    FROM {{ ref('hub_activities_svc__fact_activity') }}
)
WHERE owner_count > 1

UNION ALL

SELECT 'ambiguous_sleep_day' AS issue
FROM {{ ref('hub_health_svc__sleep_daily') }}
GROUP BY health_date
HAVING COUNT(*) > 1

UNION ALL

SELECT 'ambiguous_readiness_day' AS issue
FROM {{ ref('hub_health_svc__training_readiness_daily') }}
GROUP BY health_date
HAVING COUNT(*) > 1

UNION ALL

SELECT 'ambiguous_training_status_day' AS issue
FROM {{ ref('hub_health_svc__training_status_daily') }}
GROUP BY health_date
HAVING COUNT(*) > 1

UNION ALL

SELECT 'ambiguous_vo2max_day' AS issue
FROM {{ ref('hub_health_svc__vo2max_daily') }}
GROUP BY health_date
HAVING COUNT(*) > 1
