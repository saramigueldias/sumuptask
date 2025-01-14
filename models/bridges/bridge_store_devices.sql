SELECT DISTINCT
    id AS device_id
    , store_id
FROM {{ ref('stg_devices') }}