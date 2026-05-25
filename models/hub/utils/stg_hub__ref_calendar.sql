{{
    config(
        materialized='view'
    )
}}

-- Référence unique des bornes de périodes standard de la plateforme.
-- Toutes les fenêtres temporelles sont en jours calendaires Europe/Paris.
-- La période précédente est la fenêtre équivalente juste avant la période courante.

SELECT
    '7d' AS period,
    DATE_SUB(CURRENT_DATE('Europe/Paris'), INTERVAL 6 DAY) AS period_start,
    CURRENT_DATE('Europe/Paris') AS period_end,
    DATE_SUB(CURRENT_DATE('Europe/Paris'), INTERVAL 13 DAY) AS prev_period_start,
    DATE_SUB(CURRENT_DATE('Europe/Paris'), INTERVAL 7 DAY) AS prev_period_end

UNION ALL

SELECT
    '30d' AS period,
    DATE_SUB(CURRENT_DATE('Europe/Paris'), INTERVAL 29 DAY) AS period_start,
    CURRENT_DATE('Europe/Paris') AS period_end,
    DATE_SUB(CURRENT_DATE('Europe/Paris'), INTERVAL 59 DAY) AS prev_period_start,
    DATE_SUB(CURRENT_DATE('Europe/Paris'), INTERVAL 30 DAY) AS prev_period_end

UNION ALL

SELECT
    '6m' AS period,
    DATE_SUB(CURRENT_DATE('Europe/Paris'), INTERVAL 179 DAY) AS period_start,
    CURRENT_DATE('Europe/Paris') AS period_end,
    DATE_SUB(CURRENT_DATE('Europe/Paris'), INTERVAL 359 DAY) AS prev_period_start,
    DATE_SUB(CURRENT_DATE('Europe/Paris'), INTERVAL 180 DAY) AS prev_period_end

UNION ALL

SELECT
    '1y' AS period,
    DATE_SUB(CURRENT_DATE('Europe/Paris'), INTERVAL 364 DAY) AS period_start,
    CURRENT_DATE('Europe/Paris') AS period_end,
    DATE_SUB(CURRENT_DATE('Europe/Paris'), INTERVAL 729 DAY) AS prev_period_start,
    DATE_SUB(CURRENT_DATE('Europe/Paris'), INTERVAL 365 DAY) AS prev_period_end

UNION ALL

SELECT
    'all' AS period,
    CAST(NULL AS DATE) AS period_start,
    CURRENT_DATE('Europe/Paris') AS period_end,
    CAST(NULL AS DATE) AS prev_period_start,
    CAST(NULL AS DATE) AS prev_period_end
