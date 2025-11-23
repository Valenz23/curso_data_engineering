with source as (
    select * from {{ source("nascar","nascar_results") }}
),
base as (
    select distinct
        manu,
        car_num,
        synced_at
    from source
)

select * from base