with source as (
    select * from {{ source("nascar","nascar_results") }}
),
base as (
    select distinct
        year,
        race_num,
        track,
        track_type,
        synced_at
    from source
)

select * from base
