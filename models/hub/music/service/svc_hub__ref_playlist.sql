{{
    config(
        materialized='incremental',
        unique_key='playlist_id',
        merge_update_columns=[
            'playlist_name', 'description', 'collaborative', 'public',
            'snapshot_id', 'owner_id', 'total_tracks', 'playlist_uri', '_ingested_at'
        ],
        tags=['spotify']
    )
}}

SELECT *
FROM {{ ref('stg_hub__ref_playlist') }}

{% if is_incremental() %}
    WHERE _ingested_at > (SELECT MAX(_ingested_at) FROM {{ this }})
{% endif %}
