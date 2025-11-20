with base as (
    select * from {{ref("base_nascar__nascar_results")}}
),
selected_and_renamed as (
    select distinct
        {{ dbt_utils.generate_surrogate_key(['car_num', 'manu']) }} as car_id,
        {{ dbt_utils.generate_surrogate_key(['manu']) }} as car_manufacturer_id,
        car_num::number(2,0) as car_num
    from base
)

select * from selected_and_renamed