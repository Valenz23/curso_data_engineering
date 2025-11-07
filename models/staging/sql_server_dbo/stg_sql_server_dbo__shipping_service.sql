{{
    config(
        materialized='view'
    )
}}

with src_orders as (
    select *
    from {{ source("SQL_SERVER_DBO", "ORDERS") }}
),
renamed_casted AS (
    SELECT distinct
        md5(SHIPPING_SERVICE) as shipping_service_id,
        shipping_service as shipping_service_name,
        convert_timezone('UTC',_fivetran_synced) AS date_load_utc,
        _fivetran_deleted AS date_deleted
    FROM src_orders
    )

SELECT * FROM renamed_casted