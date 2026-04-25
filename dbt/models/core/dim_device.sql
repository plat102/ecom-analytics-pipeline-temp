{{
  config(
    materialized='table',
    schema='core'
  )
}}

WITH devices_distinct AS (
  SELECT DISTINCT
    device_category,
    browser,
    os,
  FROM {{ ref('int_events_checkout_success') }}
),

devices_with_keys AS (
  SELECT
    -- Surrogate key
    ROW_NUMBER() OVER (ORDER BY device_category, browser, os) AS device_key,

    -- Attributes
    device_category,
    browser,
    os,

    -- Derived attribute
    CASE
      WHEN device_category IN ('Mobile', 'Tablet') THEN TRUE
      ELSE FALSE
    END AS is_mobile,

  FROM devices_distinct
),

final AS (
  SELECT * FROM devices_with_keys
)


SELECT * FROM final
