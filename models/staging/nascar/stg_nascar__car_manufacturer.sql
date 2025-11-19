with base as (
    select * from {{ ref('base_nascar__nascar_results') }}
),
distinct_car_manufacturers as (
    select distinct
        manu::varchar(20) as car_manufacturer_name
    from base
),
final as (
    select
        {{ dbt_utils.generate_surrogate_key(['car_manufacturer_name']) }} as car_manufacturer_id,
        car_manufacturer_name
    from distinct_car_manufacturers
)

select * from final
