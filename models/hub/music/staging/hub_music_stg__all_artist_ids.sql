{{ config(materialized='view', tags=['spotify']) }}

-- Full known catalogue universe; enrichment worklists use the same scope as references.
SELECT DISTINCT artist_id
FROM {{ ref('hub_music_stg__artist_observations') }}
