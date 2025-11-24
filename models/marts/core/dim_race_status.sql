{{ 
    config(
        materialized='incremental', 
        unique_key='race_status_id'
    )
}}

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

{% if is_incremental() %}
  where synced_at > (select max(synced_at) from {{ this }})
{% endif %}