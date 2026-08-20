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

SELECT
    track.id AS track_id,
    track,
    added_at,
    _ingested_at
FROM {{ ref('dlk_spotify_legacy_stg__saved_tracks') }}

{% if is_incremental() %}
    WHERE _ingested_at > (SELECT MAX(_ingested_at) FROM {{ this }})
{% endif %}
