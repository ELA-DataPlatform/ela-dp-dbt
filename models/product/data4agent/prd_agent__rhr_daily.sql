-- Daily resting heart rate, one row per calendar date.

SELECT
    date AS rhr_date,
    resting_heart_rate AS resting_hr_bpm,
    _ingested_at
FROM {{ ref('svc_garmin__rhr_daily') }}
WHERE date IS NOT NULL
