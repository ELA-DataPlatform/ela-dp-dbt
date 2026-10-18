-- Lecture seule PROD : défaut de typage confirmé le 18/10/2026.
SELECT field_path, data_type
FROM `ela-dp-prd.hub_activities_svc.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS`
WHERE table_name = 'master_running_activities'
    AND field_path IN (
        'athletic_context.training_status_code',
        'start_time_local',
        'start_time_gmt'
    )
