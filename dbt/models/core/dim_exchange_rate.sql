{{
  config(
    materialized='table',
    schema='core'
  )
}}

WITH
unknown_members AS (
  SELECT
    -1 AS exchange_rate_key,
    'UNKNOWN' AS currency_code,
    1.0 AS rate_to_usd

  UNION ALL

  SELECT
    -2 AS exchange_rate_key,
    'N/A' AS currency_code,
    1.0 AS rate_to_usd
),

rates_with_keys AS (
  SELECT
    -- Surrogate key (start from 1 to avoid collision with Unknown rows)
    ROW_NUMBER() OVER (ORDER BY currency_code) AS exchange_rate_key,

    -- Attributes
    currency_code,
    rate_to_usd

  FROM {{ ref('exchange_rates') }}
),

final AS (
  SELECT * FROM unknown_members
  UNION ALL
  SELECT * FROM rates_with_keys
)

SELECT * FROM final
