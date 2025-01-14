{{
    config(
    materialized='incremental',
    unique_key=['store_id','customer_id']
    )
}}

SELECT DISTINCT
    id as store_id
    , customer_id
FROM {{ ref('stg_stores') }}
WHERE
    DATE(created_at)>='{{ var('execution_ts') }}'