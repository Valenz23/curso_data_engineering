with stg as (
    select * from {{ ref("stg_nascar__driver") }}
),
dim as (
    select       
        driver_id,
        driver_name,
        synced_at
    from stg
)

select * from dim