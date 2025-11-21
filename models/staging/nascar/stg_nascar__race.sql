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
        case
            when race_num < 0 then {{ dbt_utils.generate_surrogate_key(["'pre_season'"]) }}
            when race_num between 1 and 26 then {{ dbt_utils.generate_surrogate_key(["'regular_season'"]) }}
            when race_num between 27 and 29 then {{ dbt_utils.generate_surrogate_key(["'round_of_16'"]) }}
            when race_num between 30 and 32 then {{ dbt_utils.generate_surrogate_key(["'round_of_12'"]) }}
            when race_num between 33 and 35 then {{ dbt_utils.generate_surrogate_key(["'round_of_8'"]) }}
            when race_num=36 then {{ dbt_utils.generate_surrogate_key(["'championship_4'"]) }}
        end as race_type_id,
        {{ dbt_utils.generate_surrogate_key(['year']) }} as season_id,
        {{ dbt_utils.generate_surrogate_key(['track']) }} as track_id,
        {{ dbt_utils.generate_surrogate_key(['track_type']) }} as track_type_id,
        race_num::number(2,0) as race_num
    from selected
)

select * from renamed