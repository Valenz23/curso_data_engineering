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
        md5(promo_id) as promo_id,
        promo_id as promo_name,
        discount as discount_pct,
        status,
        convert_timezone('UTC',_fivetran_synced) AS date_load_utc,
        _fivetran_deleted AS date_deleted
    FROM src_promos
    union all 
    select
        md5(0),
        'no-promo',
        0,
        'active',
        convert_timezone('UTC', current_timestamp()),
        null
    )   
SELECT * FROM renamed_casted