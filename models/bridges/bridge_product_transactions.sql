{{
    config(
    materialized='incremental',
    unique_key=['transaction_id','product_id']
    )
}}

SELECT DISTINCT
    tr.id AS transaction_id
    , CONCAT(dv.store_id,tr.product_sku,tr.product_name) AS product_id
FROM {{ ref('stg_transactions') }} tr
JOIN {{ ref('stg_devices') }} dv
    ON tr.device_id=dv.id
WHERE
    DATE(tr.happened_at)>='{{ var('execution_ts') }}'