{{
    config(
        alias='training_readiness_daily',
        materialized='table',
        tags=['garmin', 'data4agent']
    )
}}

SELECT
    calendardate AS readiness_date,
    score AS readiness_score,
    level AS readiness_level,
    feedbackshort AS feedback_short,
    sleepscorefactorpercent AS sleep_factor_pct,
    hrvfactorpercent AS hrv_factor_pct,
    hrvweeklyaverage AS hrv_weekly_average,
    stresshistoryfactorpercent AS stress_history_factor_pct,
    recoverytimefactorpercent AS recovery_time_factor_pct,
    recoverytime AS recovery_time_hours,
    acwrfactorpercent AS acwr_factor_pct,
    acuteload AS acute_load,
    _ingested_at
FROM {{ ref('dlk_garmin_svc__training_readiness') }}
WHERE calendardate IS NOT NULL AND calendardate >= '2025-01-01'
