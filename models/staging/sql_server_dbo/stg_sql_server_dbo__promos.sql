{{
    config(
        materialized='view'
    )
}}

with src_promos as (
    select *
    from {{ source("SQL_SERVER_DBO", "PROMOS") }}
),
renamed_casted AS (
    SELECT
        md5(promo_id)::varchar(256) as promo_id,
        promo_id::varchar(256) as promo_name,
        discount::numeric(38,0) as discount_dollar,
        (case when status='active' then True else False end)::boolean as status,
        convert_timezone('UTC',_fivetran_synced) AS date_load_utc,
        _fivetran_deleted AS date_deleted
    FROM src_promos
    union all 
    select
        md5('no-promo')::varchar(256),
        'no-promo'::varchar(256),
        0::numeric(38,0),
        True::boolean,
        convert_timezone('UTC', current_timestamp()),
        null
    )   
SELECT * FROM renamed_casted