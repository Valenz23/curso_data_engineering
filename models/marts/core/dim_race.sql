with stg as (
    select * from {{ ref("int_race_season_track_track_type__joined") }}
),
dim as (
    select
        race_id,
        season,
        track_name,
        track_type,
        race_number,
        synced_at
    from stg
)
select * from dim