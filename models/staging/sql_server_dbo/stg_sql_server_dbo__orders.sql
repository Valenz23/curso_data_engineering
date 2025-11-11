{{
    config(
        materialized='view'
    )
}}

with src_orders as (
    select *
    from {{ ref('base_sql_server_dbo__orders') }}
),
renamed_casted AS (
    SELECT
        order_id,
        md5(SHIPPING_SERVICE) as shipping_service_id, 
        SHIPPING_COST,
        address_id, 
        convert_timezone('UTC',CREATED_AT) as CREATED_AT,    
        case when (PROMO_ID is null or len(PROMO_ID)=0) then md5('no-promo') else md5(PROMO_ID) end as PROMO_ID,
        convert_timezone('UTC',ESTIMATED_DELIVERY_AT) as ESTIMATED_DELIVERY_AT, -- tiene nulos
        ORDER_COST,
        USER_ID, 
        ORDER_TOTAL,
        convert_timezone('UTC',DELIVERED_AT) as DELIVERED_AT, -- tiene nulos
        TRACKING_ID, -- algunas veces no tiene valor
        STATUS, 
        convert_timezone('UTC',_fivetran_synced) AS date_load_utc,
        _fivetran_deleted AS date_deleted
    FROM src_orders
    )

SELECT * FROM renamed_casted