{{
    config(
    materialized='incremental',
    unique_key=['store_id']
    )
}}

WITH store_transactions AS (
SELECT DISTINCT
    store_id,
    case when transaction_last_status='accepted' then transaction_id end as transaction_id,
    case when transaction_last_status='accepted' then transaction_happened_at end as transaction_happened_at,
    case when transaction_last_status='accepted' then LEAD(transaction_happened_at, 5) OVER(PARTITION BY store_id ORDER BY transaction_happened_at ASC) end as fifth,
    case when transaction_last_status='accepted' then MIN(transaction_happened_at) OVER(PARTITION BY store_id ORDER BY transaction_happened_at ASC) end as min_transaction_happened_at
FROM {{ ref('fact_store_device_transactions_details') }} ft_sdt
WHERE
    DATE(ft_sdt.transaction_happened_at)>='{{ var('execution_ts') }}'
)

, five_transactions AS (
SELECT DISTINCT
    store_id
    , MIN(DATEDIFF(MIN, transaction_happened_at, fifth)) mins_diff
FROM store_transactions
WHERE transaction_happened_at=min_transaction_happened_at
GROUP BY store_id
ORDER BY store_id
)

SELECT
    ft_sdt.store_id
    , ft_sdt.store_created_at
    , ft_sdt.store_name
    , ft_sdt.store_address
    , ft_sdt.store_city
    , ft_sdt.store_country
    , ft_sdt.store_typology
    , mins_diff as minutes_to_fifth_accepted_transaction
    , count(distinct case when transaction_last_status='accepted' then transaction_id end) as nr_accepted_transactions
    , sum(case when transaction_last_status='accepted' then transaction_amount end) as total_accepted_transactions_amount
    , min(case when transaction_last_status='accepted' then transaction_happened_at end) as first_accepted_transaction_happened_at
    , current_timestamp() AS loaded_at
FROM {{ ref('fact_store_device_transactions_details') }} ft_sdt
LEFT JOIN five_transactions ft
    ON ft_sdt.store_id=ft.store_id
WHERE
    DATE(ft_sdt.transaction_happened_at)>='{{ var('execution_ts') }}'
GROUP BY ALL
