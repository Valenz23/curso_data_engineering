{{ 
    config(
        materialized='incremental', 
        unique_key='track_id'
    )
}}

with tt as (
    select * from {{ ref("stg_nascar__track_type") }}
),
joined as (
    select 
        tr.track_id as track_id,
        tr.track_name as track_name,
        tt.track_type_desc as track_type_desc,
        tr.synced_at as synced_at
    from {{ ref("stg_nascar__track") }} tr
    join tt on tt.track_type_id = tr.track_type_id

)

select * from joined

{% if is_incremental() %}
  where synced_at > (select max(synced_at) from {{ this }})
{% endif %}