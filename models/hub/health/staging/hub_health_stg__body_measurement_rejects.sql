{{ config(materialized='view') }}

-- API envelopes without a sample, timestamp or weight are not body measurements.
-- Preserve their full payload and source ingestion for investigation.
SELECT
    'weight' AS source_table,
    'not_a_measurement' AS rejection_reason,
    w._ingested_at,
    TO_JSON_STRING(w) AS source_payload
FROM {{ ref('dlk_garmin_svc__weight') }} AS w
WHERE w.samplepk IS NULL AND w.timestampgmt IS NULL AND w.weight IS NULL

UNION ALL

SELECT
    'body_composition' AS source_table,
    'not_a_measurement' AS rejection_reason,
    b._ingested_at,
    TO_JSON_STRING(b) AS source_payload
FROM {{ ref('dlk_garmin_svc__body_composition') }} AS b
WHERE b.samplepk IS NULL AND b.timestampgmt IS NULL AND b.weight IS NULL
