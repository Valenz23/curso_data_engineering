{{ 
    config(
        materialized='incremental', 
        unique_key='team_id'
    )
}}

with base as ( 
    select * from {{ ref("base_nascar__team") }}
),
renamed  as (
    select
        {{ dbt_utils.generate_surrogate_key(['team_name']) }} as team_id,
        team_name::varchar(50) as team_name,
        synced_at
    from base
)

select * from renamed

{% if is_incremental() %}
  where synced_at > (select max(synced_at) from {{ this }})
{% endif %}