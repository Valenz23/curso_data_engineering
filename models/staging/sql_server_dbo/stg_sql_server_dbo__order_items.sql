{{
    config(
        materialized='view'
    )
}}

with src_order_items as (
    select *
    from {{ source("SQL_SERVER_DBO", "ORDER_ITEMS") }}
),
renamed_casted AS (
    SELECT
        order_id,
        product_id,
        quantity,
        convert_timezone('UTC',_fivetran_synced) AS date_load_utc,
        _fivetran_deleted AS date_deleted
    FROM src_order_items
    )

SELECT * FROM renamed_casted