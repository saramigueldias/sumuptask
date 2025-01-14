{{ config(materialized='table') }}

SELECT DISTINCT
    id
    , name
    , HASH(IFNULL(address,'Unknown')) AS address
    , IFNULL(city,'Unknown') AS city
    , IFNULL(country,'Unknown') AS country
    , created_at
    , IFNULL(typology,'Unknown') AS typology
    , customer_id
FROM raw.store
WHERE
    id IS NOT NULL
    AND customer_id IS NOT NULL
    AND created_at IS NOT NULL
ORDER BY id ASC