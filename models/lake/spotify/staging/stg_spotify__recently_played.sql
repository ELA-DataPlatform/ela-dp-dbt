{{
    config(
        materialized='view',
        tags=['spotify']
    )
}}

with source as (
    select * from {{ source('spotify', 'normalized_recently_played') }}
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by played_at, track.id
            order by _ingested_at desc
        ) as _row_number
    from source
)

select * except(_row_number)
from deduplicated
where _row_number = 1
