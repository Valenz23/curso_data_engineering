with base as (
    select * from {{ref("base_nascar__nascar_results")}}
),
renamed as (
    select distinct
        {{ dbt_utils.generate_surrogate_key(['driver']) }} as driver_id,       
        driver as driver_name
    from base
)

select * from renamed