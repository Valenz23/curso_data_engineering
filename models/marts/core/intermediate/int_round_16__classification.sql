{{ 
    config(
        materialized='ephemeral', 
    )
}}

with 
rs as (select * from {{ref("int_regular_season__classification")}}),

ranked_drivers as (
    select 
        season,
        driver,
        victories,
        total_points,
        row_number() over (partition by season order by victories desc, total_points desc) as victory_rank
    from rs
),

top16 as (
    select 
        season,
        driver,
        victories,
        total_points,
        victory_rank as final_rank
    from ranked_drivers
    where victory_rank <= 16
    order by season, victory_rank
)

select * from top16