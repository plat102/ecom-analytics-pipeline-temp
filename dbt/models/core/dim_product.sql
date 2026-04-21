{{
  config(
    materialized='table',
    schema='core',
    cluster_by=['product_id']
  )
}}

WITH products_with_keys AS (
  SELECT
    -- Surrogate key
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

    -- Pricing
    currency_code,
    price,
    min_price,
    max_price,

    -- Physical attributes
    gold_weight,

  FROM {{ ref('stg_products') }}
),

final AS (
  SELECT * FROM products_with_keys
)


SELECT * FROM final
