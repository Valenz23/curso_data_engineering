with 
dr as (select * from {{ref("dim_driver")}}),
ra as (select * from {{ref("dim_race")}}),
r12 as (select * from {{ref("int_round_12__classification")}}),

r8 as (
    select top 8
        r12.season as season,
        r12.driver as driver,
        count(case when rp.final_pos=1 then 1 end) as victories,
        sum(rp.total_points) as total_points
    from {{ref("stg_nascar__race_perf")}} rp
    join dr on dr.driver_id = rp.driver_id
    join ra on ra.race_id=rp.race_id
    join r12 on r12.driver = dr.driver_name
    where ra.race_number between 30 and 32
    group by driver, r12.season
    order by victories desc, total_points desc


)

select * from r8