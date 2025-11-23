with source as (
    select * from {{ source("nascar","car") }}
),
base as (
    select 
        manu,
        car_num,
        synced_at
    from source
)

select * from base