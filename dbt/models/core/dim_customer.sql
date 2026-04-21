{{
  config(
    materialized='table',
    schema='core',
    cluster_by=['customer_natural_key']
  )
}}

WITH users_distinct AS (
  SELECT DISTINCT
    COALESCE(user_id_db, device_id) AS customer_natural_key,
    user_id_db,
    device_id,
    email_address,
  FROM {{ ref('stg_events') }}
  WHERE COALESCE(user_id_db, device_id) IS NOT NULL
),

customers_with_keys AS (
  SELECT
    -- Surrogate key
    ROW_NUMBER() OVER (ORDER BY customer_natural_key) AS customer_key,

    -- Natural key (composite)
    customer_natural_key,

    -- Attributes
    user_id_db,
    device_id,
    email_address,

  FROM users_distinct
),

final AS (
  SELECT * FROM customers_with_keys
)


SELECT * FROM final
