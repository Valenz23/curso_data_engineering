with
int_rs as (select * from {{ref("int_regular_season__classification")}}),
selected as (
    select
        season,
        driver,
        victories,
        total_points
    from int_rs
)

select * from selected