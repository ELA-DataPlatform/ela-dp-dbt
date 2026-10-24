{{ config(tags=['garmin', 'activities']) }}

-- Test each repeated family independently, without a Cartesian product.
SELECT m.activity_id
FROM {{ ref('hub_activities_svc__master_activities') }} AS m
LEFT JOIN {{ ref('hub_activities_svc__activity_recording') }} AS r ON m.activity_id = r.activity_id
LEFT JOIN {{ ref('hub_activities_svc__activity_splits') }} AS s ON m.activity_id = s.activity_id
LEFT JOIN {{ ref('hub_activities_svc__activity_zones') }} AS z ON m.activity_id = z.activity_id
LEFT JOIN {{ ref('hub_activities_svc__activity_exercise_sets') }} AS e ON m.activity_id = e.activity_id
WHERE
    ARRAY_LENGTH(m.recordings.timeseries) != COALESCE(ARRAY_LENGTH(r.timeseries), 0)
    OR ARRAY_LENGTH(m.recordings.route.polyline) IS DISTINCT FROM ARRAY_LENGTH(r.route.polyline)
    OR ARRAY_LENGTH(m.breakdown.laps) != COALESCE(ARRAY_LENGTH(s.laps), 0)
    OR ARRAY_LENGTH(m.breakdown.typed_splits) != COALESCE(ARRAY_LENGTH(s.typed_splits), 0)
    OR ARRAY_LENGTH(m.breakdown.split_summaries) != COALESCE(ARRAY_LENGTH(s.split_summaries), 0)
    OR ARRAY_LENGTH(m.zones.heart_rate) != COALESCE(ARRAY_LENGTH(z.heart_rate), 0)
    OR ARRAY_LENGTH(m.zones.power) != COALESCE(ARRAY_LENGTH(z.power), 0)
    OR ARRAY_LENGTH(m.exercise_sets) != COALESCE(ARRAY_LENGTH(e.exercise_sets), 0)
    OR m.availability.recording_response_present != (r.activity_id IS NOT NULL)
    OR m.availability.exercise_sets_response_present != (e.activity_id IS NOT NULL)
