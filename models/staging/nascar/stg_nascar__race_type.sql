with base as (
    select * from {{ ref("base_nascar__race_type") }}
)

select * from base