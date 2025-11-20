with base as (
    select * from {{ref("base_nascar__nascar_results")}}
),
selected as (
    select distinct
        {{ dbt_utils.generate_surrogate_key(['driver','car_num','manu','team_name']) }} as driver_assigment_id,
        {{ dbt_utils.generate_surrogate_key(['driver']) }} as driver_id,        
        {{ dbt_utils.generate_surrogate_key(['car_num', 'manu']) }} as car_id,
        {{ dbt_utils.generate_surrogate_key(['team_name']) }} as team_id
    from base
)

select * from selected