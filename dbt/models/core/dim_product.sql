{{
  config(
    materialized='table',
    schema='core',
    cluster_by=['product_id']
  )
}}

WITH stg_product__add_surrogate_key AS (
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
    category_id,

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
  SELECT * FROM stg_product__add_surrogate_key
)


SELECT * FROM final
