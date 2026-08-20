{{
    config(
        materialized='view',
        tags=['spotify']
    )
}}

WITH latest_ingestion AS (
    SELECT *
    FROM {{ source('spotify', 'normalized_top_tracks') }}
    WHERE _ingested_at = (
        SELECT max(_ingested_at)
        FROM {{ source('spotify', 'normalized_top_tracks') }}
    )
)

SELECT *
FROM latest_ingestion
