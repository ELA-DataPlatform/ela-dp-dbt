{{ config(materialized='view', tags=['spotify']) }}

-- Current canonical metadata; timestamp covers every contributing observation.
WITH observations AS (
    SELECT
        *,
        MAX(_ingested_at) OVER (PARTITION BY album_id) AS _last_observed_at,
        CASE
            WHEN COUNTIF(_is_detail AND album_name IS NOT NULL) OVER (PARTITION BY album_id) > 0 THEN 'complete'
            WHEN COUNTIF(_is_detail) OVER (PARTITION BY album_id) > 0 THEN 'unavailable'
            ELSE 'pending'
        END AS enrichment_status
    FROM {{ ref('hub_music_stg__album_observations') }}
)

SELECT
    album_id,
    album_name,
    album_type,
    total_tracks,
    release_date,
    release_date_precision,
    album_uri,
    _source_name AS metadata_source,
    enrichment_status,
    _last_observed_at AS _ingested_at,
    FIRST_VALUE(genres IGNORE NULLS) OVER entity_window AS genres,
    FIRST_VALUE(label IGNORE NULLS) OVER entity_window AS label,
    FIRST_VALUE(popularity IGNORE NULLS) OVER entity_window AS popularity,
    FIRST_VALUE(album_image_url IGNORE NULLS) OVER entity_window AS album_image_url
FROM observations AS o
QUALIFY
    ROW_NUMBER() OVER (
        PARTITION BY album_id
        ORDER BY
            IF(album_name IS NULL, 1, 0) ASC,
            _source_priority ASC,
            _ingested_at DESC,
            _source_name ASC,
            TO_JSON_STRING(o) ASC
    ) = 1
WINDOW entity_window AS (
    PARTITION BY album_id
    ORDER BY
        IF(album_name IS NULL, 1, 0) ASC,
        _source_priority ASC,
        _ingested_at DESC,
        _source_name ASC,
        TO_JSON_STRING(o) ASC
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
)
