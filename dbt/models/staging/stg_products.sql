{{
  config(
    materialized='view',
    schema='staging'
  )
}}

WITH products_cleaned AS (
  SELECT
    -- Primary key
    product_id,

    -- Product metadata
    NULLIF(url, '') AS url,
    NULLIF(product_name, '') AS product_name,
    NULLIF(sku, '') AS sku,

    -- Classification (use collection instead of collection_name)
    NULLIF(product_type, '') AS product_type,
    NULLIF(collection, '') AS collection_name,
    NULLIF(gender, '') AS gender,

    -- Category (convert '0' to NULL)
    CASE
      WHEN category = '0' THEN NULL
      ELSE NULLIF(category, '')
    END AS category_id,

    -- Price fields (fix invalid prices)
    NULLIF(currency_code, '') AS currency_code,

    CASE
      WHEN SAFE_CAST(price AS NUMERIC) <= 0 THEN NULL
      ELSE SAFE_CAST(price AS NUMERIC)
    END AS price,

    CASE
      WHEN SAFE_CAST(min_price AS NUMERIC) <= 0 THEN NULL
      ELSE SAFE_CAST(min_price AS NUMERIC)
    END AS min_price,

    CASE
      WHEN SAFE_CAST(max_price AS NUMERIC) <= 0 THEN NULL
      ELSE SAFE_CAST(max_price AS NUMERIC)
    END AS max_price,

    -- Physical attributes
    SAFE_CAST(gold_weight AS NUMERIC) AS gold_weight,

    -- Metadata
    NULLIF(status, '') AS status,
    ingested_at,

  FROM {{ source('glamira_raw', 'products') }}
  WHERE status = 'success'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY ingested_at DESC) = 1
),

final AS (
  SELECT * FROM products_cleaned
)


SELECT * FROM final
