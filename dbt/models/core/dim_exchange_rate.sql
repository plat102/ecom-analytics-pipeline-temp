{{
  config(
    materialized='table',
    schema='core'
  )
}}

WITH rates_with_keys AS (
  SELECT
    -- Surrogate key
    ROW_NUMBER() OVER (ORDER BY currency_code) AS exchange_rate_key,

    -- Attributes
    currency_code,
    rate_to_usd,

  FROM {{ ref('exchange_rates') }}
),

final AS (
  SELECT * FROM rates_with_keys
)


SELECT * FROM final
