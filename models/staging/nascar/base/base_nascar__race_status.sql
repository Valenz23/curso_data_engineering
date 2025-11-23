with source as (
    select * from {{ source("nascar","race_status") }}
),
base as (
    select distinct
        case 
            when lower(status) = 'running' then 'Completed'
            when lower(status) = 'running/dq' then 'DQ'
            else 'DNF'
        end as race_status_desc,
        synced_at
    from source
)

select * from base