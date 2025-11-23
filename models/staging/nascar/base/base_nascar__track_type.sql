with source as (
    select * from {{ source("nascar","track_type") }}
),
base as (
    select 
        track_type,
        synced_at
    from source
)

select * from base