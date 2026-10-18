{{ config(materialized='table', tags=['spotify']) }}

-- Rebuild current state to propagate corrections and remove obsolete relationships.
SELECT *
FROM {{ ref('hub_music_stg__ref_artist') }}
