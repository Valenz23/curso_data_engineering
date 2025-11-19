with base as (
    select * from {{ref("base_nascar__nascar_results")}}
),
selected as (
    select 
        distinct year,
        track,
        track_type,
        race_num
    from base
),
renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['year','track','track_type','race_num']) }} as race_id,
        {{ dbt_utils.generate_surrogate_key(['year']) }} as season_id,
        {{ dbt_utils.generate_surrogate_key(['track']) }} as track_id,
        {{ dbt_utils.generate_surrogate_key(['track_type']) }} as track_type_id,
        race_num::number(2,0) as race_num
    from selected
)

select * from renamed