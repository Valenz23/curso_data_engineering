with 
rs as (select * from {{ref("int_round_16__classification")}}),

top16 as (
    select
        season,
        driver,
        victories,
        total_points
    from rs
)

select * from top16