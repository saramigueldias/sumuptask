{{
    config(
    materialized='incremental',
    unique_key='id',
    on_schema_change='append_new_columns'
    )
}}

SELECT DISTINCT
    id
    , name
    , address
    , city
    , country
    , typology
    , created_at
    , current_timestamp() AS loaded_at
FROM {{ ref('stg_stores') }}
WHERE
    DATE(created_at)>='{{ var('execution_ts') }}'