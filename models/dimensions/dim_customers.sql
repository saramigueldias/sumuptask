{{
    config(
    materialized='incremental',
    unique_key='id',
    on_schema_change='append_new_columns'
    )
}}

SELECT DISTINCT
    customer_id AS id
    , FIRST_VALUE(created_at) OVER(PARTITION BY customer_id ORDER  BY created_at ASC) as created_at
    , current_timestamp() AS loaded_at
FROM {{ ref('stg_stores') }}
WHERE
    DATE(created_at)>='{{ var('execution_ts') }}'