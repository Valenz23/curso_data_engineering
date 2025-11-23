{{ 
    config(
        materialized='incremental', 
        unique_key='race_status_id'
    )
}}

with base as (
    select * from {{ ref('base_nascar__race_status') }}
),
renamed as (
    select  
        {{ dbt_utils.generate_surrogate_key(['race_status_desc']) }} as race_status_id,
        race_status_desc,
        synced_at
    from base
)

select * from renamed

{% if is_incremental() %}
  where synced_at > (select max(synced_at) from {{ this }})
{% endif %}