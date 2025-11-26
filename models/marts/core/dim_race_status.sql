with stg as (
    select * from {{ ref("stg_nascar__race_status") }}
),
dim as (
    select
        race_status_id,
        race_status_desc,
        synced_at
    from stg
)
select * from dim