{{
    config(
        materialized='incremental',
        unique_key='track_id',
        merge_update_columns=[
            'track',
            'added_at',
            '_ingested_at'
        ],
        tags=['spotify']
    )
}}

select
    track.id as track_id,
    track,
    added_at,
    _ingested_at
from {{ ref('stg_spotify__saved_tracks') }}
