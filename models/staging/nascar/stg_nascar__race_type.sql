with race_type as (
    select 'pre_season'      as col union all
    select 'regular_season'  as col union all
    select 'round_of_16'     as col union all
    select 'round_of_12'     as col union all
    select 'round_of_8'      as col union all
    select 'championship_4'  as col
),
renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['col']) }} as race_type_id,
        col      as race_type_desc
    from race_type
)

select * from renamed