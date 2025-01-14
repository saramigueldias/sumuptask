{{
    config(
    materialized='incremental',
    unique_key='id',
    on_schema_change='append_new_columns'
    )
}}

SELECT DISTINCT
    CONCAT(dv.store_id,tr.product_sku,tr.product_name) AS id
    , tr.product_sku
    , tr.product_name
FROM {{ ref('stg_transactions') }} AS tr
JOIN {{ ref('stg_devices') }} dv
    ON tr.device_id=dv.id
WHERE
    DATE(tr.happened_at)>='{{ var('execution_ts') }}'