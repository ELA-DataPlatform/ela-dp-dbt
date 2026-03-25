{{
    config(
        materialized='incremental',
        unique_key='track_id',
        merge_update_columns=[
            'track_name', 'artist_name', 'album_name', 'duration_s',
            'found', 'lrclib_id', 'instrumental', 'plain_lyrics',
            'fetched_at', '_ingested_at'
        ],
        tags=['lyrics']
    )
}}

SELECT *
FROM {{ ref('stg_lyrics__lyrics') }}

{% if is_incremental() %}
    WHERE _ingested_at > (SELECT MAX(_ingested_at) FROM {{ this }})
{% endif %}
