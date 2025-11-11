{{
    config(
        materialized='view'
    )
}}

with src_orders as (
    select *
    from {{ ref("base_sql_server_dbo_shipping_service") }}
),
renamed_casted AS (
    SELECT distinct
        md5(SHIPPING_SERVICE) as shipping_service_id,
        case when len(shipping_service)=0 then 'unknown' else shipping_service end as shipping_service_name,
        convert_timezone('UTC',_fivetran_synced) AS date_load_utc,
        _fivetran_deleted AS date_deleted
    FROM src_orders
    )

SELECT * FROM renamed_casted