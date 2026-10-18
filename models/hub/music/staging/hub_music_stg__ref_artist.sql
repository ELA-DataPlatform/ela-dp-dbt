{{ config(materialized='view', tags=['spotify']) }}

-- Current canonical metadata; timestamp covers every contributing observation.
WITH observations AS (
    SELECT
        *,
        MAX(_ingested_at) OVER (PARTITION BY artist_id) AS _last_observed_at,
        CASE
            WHEN COUNTIF(_is_detail AND artist_name IS NOT NULL) OVER (PARTITION BY artist_id) > 0 THEN 'complete'
            WHEN COUNTIF(_is_detail) OVER (PARTITION BY artist_id) > 0 THEN 'unavailable'
            ELSE 'pending'
        END AS enrichment_status
    FROM {{ ref('hub_music_stg__artist_observations') }}
)

SELECT
    artist_id,
    artist_name,
    artist_uri,
    _source_name AS metadata_source,
    enrichment_status,
    _last_observed_at AS _ingested_at,
    FIRST_VALUE(genres IGNORE NULLS) OVER entity_window AS genres,
    FIRST_VALUE(followers_total IGNORE NULLS) OVER entity_window AS followers_total,
    FIRST_VALUE(popularity IGNORE NULLS) OVER entity_window AS popularity,
    FIRST_VALUE(artist_image_url IGNORE NULLS) OVER entity_window AS artist_image_url
FROM observations AS o
QUALIFY
    ROW_NUMBER() OVER (
        PARTITION BY artist_id
        ORDER BY
            IF(artist_name IS NULL, 1, 0) ASC,
            _source_priority ASC,
            _ingested_at DESC,
            _source_name ASC,
            TO_JSON_STRING(o) ASC
    ) = 1
WINDOW entity_window AS (
    PARTITION BY artist_id
    ORDER BY
        IF(artist_name IS NULL, 1, 0) ASC,
        _source_priority ASC,
        _ingested_at DESC,
        _source_name ASC,
        TO_JSON_STRING(o) ASC
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
)
