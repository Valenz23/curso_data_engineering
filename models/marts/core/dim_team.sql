{{ 
    config(
        materialized='incremental', 
        unique_key='team_id'
    )
}}

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

{% if is_incremental() %}
  where synced_at > (select max(synced_at) from {{ this }})
{% endif %}