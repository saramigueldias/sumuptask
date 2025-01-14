{{
    config(
    materialized='table'
    )
}}

SELECT DISTINCT
    id
    , type
    , current_timestamp() AS loaded_at
FROM {{ ref('stg_devices') }}
