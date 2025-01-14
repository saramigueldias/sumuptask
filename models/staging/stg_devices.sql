{{ config(materialized='table') }}

SELECT DISTINCT
    id
    , IFNULL(type, 'Unknown') AS type
    , store_id
FROM raw.device
WHERE
    id IS NOT NULL
    AND store_id IS NOT NULL
ORDER BY id ASC
