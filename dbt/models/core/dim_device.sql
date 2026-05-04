{{
  config(
    materialized='table',
    schema='core'
  )
}}

WITH int_event__get_distinct_device AS (
  SELECT DISTINCT
    device_category,
    browser,
    os,
  FROM {{ ref('int_events_checkout_success') }}
),

int_event__add_surrogate_key AS (
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

  FROM int_event__get_distinct_device
),

final AS (
  SELECT * FROM int_event__add_surrogate_key
)


SELECT * FROM final
