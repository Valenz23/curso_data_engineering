with source as (
    select * from {{ source("nascar","team") }}
),
base as (
    select
        team_name,
        synced_at
    from source
)

select * from base