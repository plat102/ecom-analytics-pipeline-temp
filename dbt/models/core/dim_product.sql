{{
  config(
    materialized='table',
    schema='core',
    cluster_by=['product_id']
  )
}}

WITH
unknown_members AS (
  SELECT
    -1 AS product_key,
    'UNKNOWN' AS product_id,
    'Unknown' AS url,
    'Unknown Product' AS product_name,
    'UNKNOWN' AS sku,
    'Unknown' AS product_type,
    'Unknown' AS collection_name,
    'Unknown' AS gender,
    '-1' AS category_id,
    'USD' AS currency_code,
    0.0 AS price,
    0.0 AS min_price,
    0.0 AS max_price,
    0.0 AS gold_weight

  UNION ALL

  SELECT
    -2 AS product_key,
    'N/A' AS product_id,
    'N/A' AS url,
    'Not Applicable' AS product_name,
    'N/A' AS sku,
    'N/A' AS product_type,
    'N/A' AS collection_name,
    'N/A' AS gender,
    '-2' AS category_id,
    'USD' AS currency_code,
    0.0 AS price,
    0.0 AS min_price,
    0.0 AS max_price,
    0.0 AS gold_weight
),

stg_product__add_surrogate_key AS (
  SELECT
    -- Surrogate key (start from 1 to avoid collision with Unknown rows)
    ROW_NUMBER() OVER (ORDER BY product_id) AS product_key,

    -- Natural key
    product_id,

    -- Product attributes
    url,
    product_name,
    sku,
    product_type,
    collection_name,
    gender,
    category_id,

    -- Pricing
    currency_code,
    price,
    min_price,
    max_price,

    -- Physical attributes
    gold_weight

  FROM {{ ref('stg_products') }}
),

final AS (
  SELECT * FROM unknown_members
  UNION ALL
  SELECT * FROM stg_product__add_surrogate_key
)

SELECT * FROM final
