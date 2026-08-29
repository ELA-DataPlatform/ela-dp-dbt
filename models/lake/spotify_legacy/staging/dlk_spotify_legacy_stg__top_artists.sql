{{
    config(
        materialized='view',
        tags=['spotify']
    )
}}

WITH source AS (
    SELECT
        id,
        popularity,
        name,
        images,
        uri,
        type,
        href,
        genres,
        followers,
        external_urls,
        _ingested_at
    FROM {{ source('spotify_legacy', 'normalized_top_artists_legacy') }}
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY _ingested_at DESC
        ) AS _row_number
    FROM source
)

SELECT * EXCEPT (_row_number)
FROM deduplicated
WHERE _row_number = 1
