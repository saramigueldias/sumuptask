{{
    config(
    materialized='incremental',
    unique_key=['transaction_id','device_id']
    )
}}

SELECT DISTINCT
    id AS transaction_id
    , device_id
FROM {{ ref('stg_transactions') }} tr
WHERE
    DATE(happened_at)>='{{ var('execution_ts') }}'