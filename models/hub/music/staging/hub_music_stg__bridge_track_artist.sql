{{ config(materialized='view', tags=['spotify']) }}

-- Select one entire artist list per entity before expanding it.
-- Deduplicate repeated artist IDs within that list and retain their first position.
WITH candidates AS (
    SELECT
        track_id,
        _source_priority,
        _source_name,
        _ingested_at,
        ARRAY(
            SELECT AS STRUCT
                JSON_VALUE(artist, '$.id') AS artist_id,
                MIN(artist_position) AS source_position
            FROM UNNEST(JSON_QUERY_ARRAY(o.artists)) AS artist WITH OFFSET AS artist_position
            WHERE NULLIF(TRIM(JSON_VALUE(artist, '$.id')), '') IS NOT NULL
            GROUP BY artist_id
            ORDER BY source_position, artist_id
        ) AS artists
    FROM {{ ref('hub_music_stg__track_observations') }} AS o
),

canonical AS (
    SELECT *
    FROM candidates AS c
    WHERE ARRAY_LENGTH(artists) > 0 AND NULLIF(TRIM(track_id), '') IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY track_id
        ORDER BY _source_priority ASC, _ingested_at DESC, _source_name ASC, TO_JSON_STRING(c) ASC
    ) = 1
)

SELECT
    c.track_id,
    artist.artist_id,
    artist_position,
    c._ingested_at
FROM canonical AS c,
    UNNEST(c.artists) AS artist WITH OFFSET AS artist_position
