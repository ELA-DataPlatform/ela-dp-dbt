{{ config(materialized='view', tags=['spotify']) }}

-- Current canonical metadata; timestamp covers every contributing observation.
WITH observations AS (
    SELECT
        *,
        MAX(_ingested_at) OVER (PARTITION BY track_id) AS _last_observed_at
    FROM {{ ref('hub_music_stg__track_observations') }}
    WHERE NULLIF(TRIM(track_id), '') IS NOT NULL
)

SELECT
    track_id,
    track_name,
    track_uri,
    explicit,
    track_number,
    disc_number,
    album_id,
    is_local,
    _source_name AS metadata_source,
    _last_observed_at AS _ingested_at,
    FIRST_VALUE(duration_ms IGNORE NULLS) OVER entity_window AS duration_ms,
    FIRST_VALUE(popularity IGNORE NULLS) OVER entity_window AS popularity,
    FIRST_VALUE(isrc IGNORE NULLS) OVER entity_window AS isrc
FROM observations AS o
QUALIFY
    ROW_NUMBER() OVER (
        PARTITION BY track_id
        ORDER BY
            IF(track_name IS NULL, 1, 0) ASC,
            _source_priority ASC,
            _ingested_at DESC,
            _source_name ASC,
            TO_JSON_STRING(o) ASC
    ) = 1
WINDOW entity_window AS (
    PARTITION BY track_id
    ORDER BY
        IF(track_name IS NULL, 1, 0) ASC,
        _source_priority ASC,
        _ingested_at DESC,
        _source_name ASC,
        TO_JSON_STRING(o) ASC
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
)
