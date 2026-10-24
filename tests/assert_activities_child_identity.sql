{{ config(tags=['garmin', 'activities']) }}

SELECT activity_id, 'duplicate_sample_offset' AS issue
FROM {{ ref('hub_activities_svc__activity_recording') }}, UNNEST(timeseries) AS p
GROUP BY activity_id, p.sample_index
HAVING COUNT(*) > 1

UNION ALL

SELECT activity_id, 'duplicate_lap_offset' AS issue
FROM {{ ref('hub_activities_svc__activity_splits') }}, UNNEST(laps) AS p
GROUP BY activity_id, p.source_index
HAVING COUNT(*) > 1

UNION ALL

SELECT activity_id, 'duplicate_set_offset' AS issue
FROM {{ ref('hub_activities_svc__activity_exercise_sets') }}, UNNEST(exercise_sets) AS p
GROUP BY activity_id, p.set_index
HAVING COUNT(*) > 1
