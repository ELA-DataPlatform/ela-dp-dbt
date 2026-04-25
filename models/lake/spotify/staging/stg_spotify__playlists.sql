{{
    config(
        materialized='view',
        tags=['spotify']
    )
}}

WITH latest_ingestion AS (
    SELECT *
    FROM {{ source('spotify', 'normalized_playlists') }}
    WHERE _ingested_at = (
        SELECT max(_ingested_at)
        FROM {{ source('spotify', 'normalized_playlists') }}
    )
)

SELECT *
FROM latest_ingestion
