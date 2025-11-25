with 
rs as (select * from {{ref("int_round_12__classification")}}),

top12 as (
    select
        season,
        driver,
        victories,
        total_points
    from rs
)

select * from top12