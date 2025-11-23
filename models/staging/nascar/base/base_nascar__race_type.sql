with source as (
    select * from {{ source("nascar","race_type") }}
),
base as (
    select 
        race_type_id,
        race_type_desc,
        synced_at
    from source
)

select * from base