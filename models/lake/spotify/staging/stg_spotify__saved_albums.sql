{{
    config(
        materialized='view',
        tags=['spotify']
    )
}}

WITH latest_ingestion AS (
    SELECT *
    FROM {{ source('spotify', 'normalized_saved_albums') }}
    WHERE _ingested_at = (
        SELECT max(_ingested_at)
        FROM {{ source('spotify', 'normalized_saved_albums') }}
    )
)

SELECT *
FROM latest_ingestion
