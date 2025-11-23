with source as (
    select * from {{ source("nascar","nascar_results") }}
),
base as (
    select distinct
        year,
        race_num,
        driver,
        car_num,
        manu,
        team_name,
        status,
        "start",
        fin,
        stage_1,
        stage_2,
        stage_3_or_duel,
        laps,
        laps_led,
        points,
        stage_points,
        synced_at
    from source
)

select * from base
