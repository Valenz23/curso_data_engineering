with
top1_rs as (select top 1 season, driver from {{ ref("int_regular_season__classification") }}),
winner_nc as (select top 1 season, driver from {{ ref("int_champ_4__winner") }}),
comparator as (
    select 
        winner_nc.season as season,
        top1_rs.driver as best_reg_season,
        winner_nc.driver as nascar_champ,
        (best_reg_season=nascar_champ) as same_driver
    from winner_nc
    join top1_rs on top1_rs.season=winner_nc.season
)

select * from comparator