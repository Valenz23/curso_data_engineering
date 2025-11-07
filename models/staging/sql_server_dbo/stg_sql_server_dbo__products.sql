{{
    config(
        materialized='view'
    )
}}

with src_product as (
    select *
    from {{ source("SQL_SERVER_DBO", "PRODUCTS") }}
),
renamed_casted AS (
    SELECT
        product_id,
        name,
        price,
        inventory, -- este que?
        convert_timezone('UTC',_fivetran_synced) AS date_load_utc,
        _fivetran_deleted AS date_deleted
    FROM src_product
    )

SELECT * FROM renamed_casted