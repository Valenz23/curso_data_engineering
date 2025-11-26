with
ranked_rs as (
    select 
        season, 
        driver,
        victories,
        total_points,
        row_number() over (partition by season order by victories desc, total_points desc) as rank
    from {{ ref("int_regular_season__classification") }}
),
top1_rs as (
    select 
        season, 
        driver
    from ranked_rs
    where rank = 1
),
c4_ranked as (
    select
        season, 
        driver,        
        row_number() over (partition by season order by final_pos asc) as rank
    from {{ ref("int_champ_4__winner") }}
),
top1_nc as (
    select 
        season, 
        driver
    from c4_ranked
    where rank = 1
),
comparator as (
    select 
        top1_nc.season as season,
        top1_rs.driver as best_reg_season,
        top1_nc.driver as nascar_champ,
        (top1_rs.driver = top1_nc.driver) as same_driver
    from top1_nc
    join top1_rs on top1_rs.season = top1_nc.season
)

select * from comparator