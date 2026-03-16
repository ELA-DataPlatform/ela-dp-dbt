{{
    config(
        materialized='view',
        tags=['spotify']
    )
}}

with latest_ingestion as (
    select *
    from {{ source('spotify', 'normalized_saved_albums') }}
    where _ingested_at = (
        select max(_ingested_at)
        from {{ source('spotify', 'normalized_saved_albums') }}
    )
)

select *
from latest_ingestion
