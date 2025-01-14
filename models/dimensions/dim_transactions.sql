{{
    config(
    materialized='incremental',
    unique_key='id',
    on_schema_change='append_new_columns'
    )
}}

SELECT DISTINCT
    id
    , category_name
    , amount
    , status
    , card_number
    , cvv
    , created_at
    , happened_at
    , current_timestamp() AS loaded_at
FROM {{ ref('stg_transactions') }}
WHERE
    DATE(happened_at)>='{{ var('execution_ts') }}'