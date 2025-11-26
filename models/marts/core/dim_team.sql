with stg as (
    select * from {{ ref("stg_nascar__team") }}
),
dim as (
    select
        team_id,
        team_name,
        synced_at
    from stg
)
select * from dim