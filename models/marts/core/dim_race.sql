{{ 
    config(
        materialized='incremental', 
        unique_key='race_id'
    )
}}

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

{% if is_incremental() %}
  where synced_at > (select max(synced_at) from {{ this }})
{% endif %}