with source as (
    select * from {{ source("nascar","race") }}
),
base as (
    select
        year,
        race_num,
        track,
        synced_at
    from source
)

select * from base
