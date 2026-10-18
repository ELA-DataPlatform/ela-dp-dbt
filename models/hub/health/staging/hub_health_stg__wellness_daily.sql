{{ config(materialized='view') }}

-- One daily movement and energy summary per Garmin source profile, from user_summary only. Intensity minutes are not
-- summed or weighted across endpoints.
WITH normalized AS (
    SELECT
        s.date AS health_date,
        'profile' AS identity_scope,
        'garmin' AS source_system,
        'user_summary' AS source_table,
        s._ingested_at,
        s.calendardate AS calendar_date,
        s.includeswellnessdata AS includes_wellness_data,
        s.totalsteps AS steps,
        s.dailystepgoal AS step_goal,
        s.totaldistancemeters AS distance_m,
        s.totalkilocalories AS total_kcal,
        s.activekilocalories AS active_kcal,
        s.bmrkilocalories AS bmr_kcal,
        s.floorsascended AS floors_ascended,
        s.floorsdescended AS floors_descended,
        s.moderateintensityminutes AS moderate_intensity_min,
        s.vigorousintensityminutes AS vigorous_intensity_min,
        s.activeseconds AS active_s,
        s.sedentaryseconds AS sedentary_s,
        s.sleepingseconds AS sleep_s,
        s.wellnessstarttimegmt AS start_at_utc,
        s.wellnessendtimegmt AS end_at_utc,
        CAST(s.userprofileid AS STRING) AS source_user_id,
        TO_JSON_STRING(STRUCT(s.date AS source_date, s.userprofileid AS source_user_id)) AS source_record_key
    FROM {{ ref('dlk_garmin_svc__user_summary') }} AS s
),

qualified AS (
    SELECT
        *,
        ARRAY_CONCAT(
            IF(health_date IS NULL, ['missing_health_date'], ARRAY<STRING>[]),
            IF(source_user_id IS NULL, ['missing_source_user_id'], ARRAY<STRING>[]),
            IF(steps IS NOT NULL AND NOT (steps >= 0), ['invalid_steps'], ARRAY<STRING>[]),
            IF(step_goal IS NOT NULL AND NOT (step_goal >= 0), ['invalid_step_goal'], ARRAY<STRING>[]),
            IF(distance_m IS NOT NULL AND NOT (distance_m >= 0), ['invalid_distance_m'], ARRAY<STRING>[]),
            IF(total_kcal IS NOT NULL AND NOT (total_kcal >= 0), ['invalid_total_kcal'], ARRAY<STRING>[]),
            IF(active_kcal IS NOT NULL AND NOT (active_kcal >= 0), ['invalid_active_kcal'], ARRAY<STRING>[]),
            IF(bmr_kcal IS NOT NULL AND NOT (bmr_kcal >= 0), ['invalid_bmr_kcal'], ARRAY<STRING>[]),
            IF(
                floors_ascended IS NOT NULL AND NOT (floors_ascended >= 0), ['invalid_floors_ascended'], ARRAY<STRING>[]
            ),
            IF(
                floors_descended IS NOT NULL AND NOT (floors_descended >= 0),
                ['invalid_floors_descended'],
                ARRAY<STRING>[]
            ),
            IF(
                moderate_intensity_min IS NOT NULL AND NOT (moderate_intensity_min >= 0),
                ['invalid_moderate_intensity_min'],
                ARRAY<STRING>[]
            ),
            IF(
                vigorous_intensity_min IS NOT NULL AND NOT (vigorous_intensity_min >= 0),
                ['invalid_vigorous_intensity_min'],
                ARRAY<STRING>[]
            ),
            IF(active_s IS NOT NULL AND NOT (active_s >= 0), ['invalid_active_s'], ARRAY<STRING>[]),
            IF(sedentary_s IS NOT NULL AND NOT (sedentary_s >= 0), ['invalid_sedentary_s'], ARRAY<STRING>[]),
            IF(sleep_s IS NOT NULL AND NOT (sleep_s >= 0), ['invalid_sleep_s'], ARRAY<STRING>[])
        ) AS _invalid_issues,
        ARRAY_CONCAT(
            IF(steps IS NULL, ['missing_steps'], ARRAY<STRING>[]),
            IF(includes_wellness_data IS FALSE, ['source_reports_no_wellness_data'], ARRAY<STRING>[])
        ) AS _missing_issues
    FROM normalized
)

SELECT
    health_date,
    source_user_id,
    identity_scope,
    source_system,
    source_table,
    source_record_key,
    calendar_date,
    includes_wellness_data,
    start_at_utc,
    end_at_utc,
    _ingested_at,
    IF(steps >= 0, steps, NULL) AS steps,
    IF(step_goal >= 0, step_goal, NULL) AS step_goal,
    IF(distance_m >= 0, distance_m, NULL) AS distance_m,
    IF(total_kcal >= 0, total_kcal, NULL) AS total_kcal,
    IF(active_kcal >= 0, active_kcal, NULL) AS active_kcal,
    IF(bmr_kcal >= 0, bmr_kcal, NULL) AS bmr_kcal,
    IF(floors_ascended >= 0, floors_ascended, NULL) AS floors_ascended,
    IF(floors_descended >= 0, floors_descended, NULL) AS floors_descended,
    IF(moderate_intensity_min >= 0, moderate_intensity_min, NULL) AS moderate_intensity_min,
    IF(vigorous_intensity_min >= 0, vigorous_intensity_min, NULL) AS vigorous_intensity_min,
    IF(active_s >= 0, active_s, NULL) AS active_s,
    IF(sedentary_s >= 0, sedentary_s, NULL) AS sedentary_s,
    IF(sleep_s >= 0, sleep_s, NULL) AS sleep_s,
    STRUCT(
        calendar_date,
        includes_wellness_data,
        steps,
        step_goal,
        distance_m,
        total_kcal,
        active_kcal,
        bmr_kcal,
        floors_ascended,
        floors_descended,
        moderate_intensity_min,
        vigorous_intensity_min,
        active_s,
        sedentary_s,
        sleep_s,
        start_at_utc,
        end_at_utc
    ) AS source_values,
    CASE
        WHEN ARRAY_LENGTH(_invalid_issues) > 0 THEN 'invalid' WHEN
            ARRAY_LENGTH(_missing_issues) > 0
            THEN 'partial'
        ELSE 'valid'
    END AS quality_status,
    ARRAY_CONCAT(_invalid_issues, _missing_issues) AS quality_issues
FROM qualified AS q
QUALIFY health_date IS NULL OR ROW_NUMBER() OVER (
    PARTITION BY source_user_id, health_date
    ORDER BY _ingested_at DESC, TO_JSON_STRING(q) ASC
) = 1
