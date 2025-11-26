{{ 
    config(
        materialized='ephemeral', 
    )
}}

with 
dr as (select * from {{ref("dim_driver")}}),
ra as (select * from {{ref("dim_race")}}),
r16 as (select * from {{ref("int_round_16__classification")}}),

driver_stats as (
    select
        r16.season as season,
        r16.driver as driver,
        count(case when rp.final_pos = 1 then 1 end) as victories,
        sum(rp.total_points) as total_points
    from {{ref("stg_nascar__race_perf")}} rp
    join dr on dr.driver_id = rp.driver_id
    join ra on ra.race_id = rp.race_id
    join r16 on r16.driver = dr.driver_name
    where ra.race_number between 27 and 29
        and ra.season=r16.season
    group by r16.season, r16.driver
),
ranked_drivers as (
    select
        season,
        driver,
        victories,
        total_points,
        row_number() over (
            partition by season
            order by victories desc, total_points desc
        ) as victory_rank
    from driver_stats
),

top12 as (
    select 
        season,
        driver,
        victories,
        total_points,
        victory_rank
    from ranked_drivers
    where victory_rank <= 12
    order by season, victory_rank
)

select * from top12