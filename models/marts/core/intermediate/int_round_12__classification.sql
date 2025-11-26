{{ 
    config(
        materialized='ephemeral', 
    )
}}

with 
dr as (select * from {{ref("dim_driver")}}),
ra as (select * from {{ref("dim_race")}}),
r16 as (select * from {{ref("int_round_16__classification")}}),

r12 as (
    select top 12
        r16.season as season,
        r16.driver as driver,
        count(case when rp.final_pos=1 then 1 end) as victories,
        sum(rp.total_points) as total_points
    from {{ref("stg_nascar__race_perf")}} rp
    join dr on dr.driver_id = rp.driver_id
    join ra on ra.race_id=rp.race_id
    join r16 on r16.driver = dr.driver_name
    where ra.race_number between 27 and 29
    group by driver, r16.season
    order by victories desc, total_points desc


)

select * from r12