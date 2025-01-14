{{ config(materialized='table') }}

SELECT DISTINCT
    id
    , device_id
    , IFNULL(INITCAP(REGEXP_REPLACE(product_name,'[^:^0-9A-Za-z ]','')),'Unknown') AS product_name
    , IFNULL(product_sku,'Unknown') AS product_sku
    , IFNULL(INITCAP(REGEXP_REPLACE(category_name,'[^:^0-9A-Za-z ]','')),'Unknown') AS category_name
    , IFNULL(amount,-9999) AS amount
    , status
    , HASH(IFNULL(REGEXP_REPLACE(card_number,'[^A-Za-z0-9]',''),'Unknown')) AS card_number
    , HASH(IFNULL(cvv,-9999)) AS cvv
    , created_at
    , IFNULL(happened_at,created_at) AS happened_at
FROM raw.transaction
WHERE
    id IS NOT NULL
    AND device_id IS NOT NULL
    AND created_at IS NOT NULL
ORDER BY id ASC