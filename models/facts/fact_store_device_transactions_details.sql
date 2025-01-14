{{
    config(
    materialized='incremental',
    unique_key=['transaction_id','store_id','customer_id','device_id','product_id'],
    on_schema_change='append_new_columns'
    )
}}

SELECT DISTINCT
    tr.id as transaction_id
    , str.id as store_id
    , ct.id as customer_id
    , dv.id as device_id
    , pd.id as product_id
    , tr.category_name as transaction_category_name
    , tr.amount/sum(tr.amount) OVER(PARTITION BY product_id) as transaction_amount
    , tr.status as transaction_last_status
    , tr.card_number  as transaction_card_number
    , tr.cvv as transaction_cvv
    , tr.created_at  as transaction_created_at
    , tr.happened_at as transaction_happened_at
    , str.name as store_name
    , str.address as store_address
    , str.city as store_city
    , str.country as store_country
    , str.typology as store_typology
    , str.created_at as store_created_at
    , dv.type as device_type
    , pd.product_sku
    , pd.product_name
    , current_timestamp() AS loaded_at
FROM {{ ref('dim_stores') }} str
JOIN {{ ref('bridge_store_products') }} bg_str_pd
    ON str.id=bg_str_pd.store_id
JOIN {{ ref('dim_products') }} pd
    ON pd.id=bg_str_pd.product_id
JOIN {{ ref('bridge_store_customers') }} bg_str_ct
    ON str.id=bg_str_ct.store_id
JOIN {{ ref('dim_customers') }} ct
    ON ct.id=bg_str_ct.customer_id
JOIN {{ ref('bridge_store_devices') }} bg_str_dv
    ON str.id=bg_str_dv.store_id
JOIN {{ ref('dim_devices') }} dv
    ON dv.id=bg_str_dv.device_id
JOIN {{ ref('bridge_device_transactions') }} bg_dv_tr
    ON dv.id=bg_dv_tr.device_id
JOIN {{ ref('dim_transactions') }} tr
    ON tr.id=bg_dv_tr.transaction_id
WHERE
    DATE(tr.happened_at)>='{{ var('execution_ts') }}'
