with source as (
    select * from {{ source("nascar","track") }}
),
base as (
    select 
        track,
        track_type,
        synced_at
    from source
)

select * from base