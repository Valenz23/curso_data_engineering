with source as (
    select * from {{ source("nascar","car_manufacturer") }}
),
base as (
    select 
        manu,
        synced_at
    from source
)

select * from base