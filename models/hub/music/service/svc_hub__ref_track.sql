{{
    config(
        materialized='incremental',
        unique_key='track_id',
        merge_update_columns=[
            'track_name', 'track_uri', 'duration_ms', 'explicit',
            'track_number', 'disc_number', 'popularity', 'isrc',
            'album_id', 'is_local', '_ingested_at'
        ],
        tags=['spotify']
    )
}}

SELECT *
FROM {{ ref('stg_hub__ref_track') }}

{% if is_incremental() %}
    WHERE _ingested_at > (SELECT MAX(_ingested_at) FROM {{ this }})
{% endif %}
