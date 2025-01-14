{{
    config(
    materialized='incremental',
    unique_key='device_type'
    )
}}

WITH device_transactions AS (
SELECT DISTINCT
    device_type,
    device_id,
    store_id,
    transaction_happened_at,
    transaction_last_status,
    100*RATIO_TO_REPORT(transaction_amount) OVER(partition by device_type) AS ratio_transactions
FROM {{ ref('fact_store_device_transactions_details') }} ft_sdt
WHERE
    DATE(ft_sdt.transaction_happened_at)>='{{ var('execution_ts') }}'
)

SELECT DISTINCT
    device_type,
    COUNT(DISTINCT store_id) AS nr_stores_is_in,
    ROUND(SUM(ratio_transactions),2) ratio_transactions,
    MIN(transaction_happened_at) AS first_transaction_at,
    MAX(transaction_happened_at) AS last_transaction_at
FROM device_transactions
GROUP BY device_type
