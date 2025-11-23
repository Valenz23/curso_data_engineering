with source as (
    select * from {{ source("nascar","season") }}
),
base as (
    select
        year,
        synced_at
    from source
)

select * from base