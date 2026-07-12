{{
    config(
        materialized='table',
        tags=['garmin', 'webapp']
    )
}}

WITH date_spine AS (
    SELECT
        date_add(
            date_sub(current_date('Europe/Paris'), INTERVAL 9 DAY),
            INTERVAL n DAY
        ) AS activity_date
    FROM unnest(generate_array(0, 9)) AS n
),

sleep_data AS (
    SELECT
        date AS activity_date,
        sleep_overall_score AS sleep_score,
        cast({{ seconds_to_minutes('sleep_time_seconds', 0) }} AS int64) AS sleep_duration_minutes,
        cast(round(avgovernighthrv) AS int64) AS hrv_morning_ms,
        if(
            array_length(sleep_body_battery) > 0,
            sleep_body_battery[offset(0)].value,
            NULL
        ) AS body_battery_at_sleep,
        if(
            array_length(sleep_body_battery) > 0,
            sleep_body_battery[ordinal(array_length(sleep_body_battery))].value,
            NULL
        ) AS body_battery_at_wake
    FROM {{ ref('svc_garmin__sleep') }}
    WHERE date >= date_sub(current_date('Europe/Paris'), INTERVAL 9 DAY)
),

joined AS (
    SELECT
        d.activity_date,
        s.sleep_score,
        s.sleep_duration_minutes,
        s.hrv_morning_ms,
        s.body_battery_at_sleep,
        s.body_battery_at_wake
    FROM date_spine AS d
    LEFT JOIN sleep_data AS s ON d.activity_date = s.activity_date
),

latest_date AS (
    SELECT max(activity_date) AS latest
    FROM joined
    WHERE sleep_score IS NOT NULL OR sleep_duration_minutes IS NOT NULL
),

averages AS (
    SELECT
        round(avg(sleep_score), 1) AS avg_sleep_score,
        round(avg(sleep_duration_minutes), 1) AS avg_sleep_duration_minutes,
        round(avg(hrv_morning_ms), 1) AS avg_hrv_ms,
        round(avg(body_battery_at_wake), 1) AS avg_body_battery_wake
    FROM joined
    CROSS JOIN latest_date AS ld
    WHERE activity_date < ld.latest
),

deltas AS (
    SELECT
        cast(j.sleep_duration_minutes - a.avg_sleep_duration_minutes AS int64) AS delta_sleep_minutes,
        round(j.sleep_score - a.avg_sleep_score, 1) AS delta_sleep_score,
        round(j.hrv_morning_ms - a.avg_hrv_ms, 1) AS delta_hrv_ms,
        round(j.body_battery_at_wake - a.avg_body_battery_wake, 1) AS delta_body_battery
    FROM joined AS j
    CROSS JOIN latest_date AS ld
    CROSS JOIN averages AS a
    WHERE j.activity_date = ld.latest
)

SELECT
    -- ── KPIs agrégés (plats) ────────────────────────────────────────────────────
    a.avg_sleep_score,
    a.avg_sleep_duration_minutes,
    a.avg_hrv_ms,
    a.avg_body_battery_wake,
    d.delta_sleep_score,
    d.delta_sleep_minutes,
    d.delta_hrv_ms,
    d.delta_body_battery,
    CASE
        WHEN d.delta_sleep_score >= 5 THEN 'success'
        WHEN d.delta_sleep_score >= 0 THEN 'neutral'
        WHEN d.delta_sleep_score >= -10 THEN 'warning'
        ELSE 'danger'
    END AS delta_sleep_score_tone,
    CASE
        WHEN d.delta_body_battery >= 5 THEN 'success'
        WHEN d.delta_body_battery >= 0 THEN 'neutral'
        WHEN d.delta_body_battery >= -10 THEN 'warning'
        ELSE 'danger'
    END AS delta_body_battery_tone,
    round(safe_divide(d.delta_sleep_score, a.avg_sleep_score) * 100, 1) AS delta_sleep_score_pct,
    round(safe_divide(d.delta_sleep_minutes, a.avg_sleep_duration_minutes) * 100, 1) AS delta_sleep_minutes_pct,
    round(safe_divide(d.delta_hrv_ms, a.avg_hrv_ms) * 100, 1) AS delta_hrv_pct,
    round(safe_divide(d.delta_body_battery, a.avg_body_battery_wake) * 100, 1) AS delta_body_battery_pct,

    -- ── Détail quotidien (ARRAY<STRUCT>) ─────────────────────────────────────────
    array(
        SELECT AS STRUCT
            j.activity_date,
            j.sleep_score,
            j.sleep_duration_minutes,
            j.hrv_morning_ms,
            j.body_battery_at_sleep,
            j.body_battery_at_wake,
            {{ format_day_label('j.activity_date') }} AS day_label,
            {{ dayofweek_to_french_letter('j.activity_date') }} AS day_letter
        FROM joined AS j
        ORDER BY j.activity_date
    ) AS days
FROM averages AS a
CROSS JOIN deltas AS d
