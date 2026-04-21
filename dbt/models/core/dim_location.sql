{{
  config(
    materialized='table',
    schema='core',
    cluster_by=['ip']
  )
}}

WITH locations_with_keys AS (
  SELECT
    -- Surrogate key
    ROW_NUMBER() OVER (ORDER BY ip) AS location_key,

    -- Natural key
    ip,

    -- Geographic attributes
    country_name,
    city_name,

    -- Quality indicators (from staging)
    geo_completeness_level,
    has_geo_data,

  FROM {{ ref('stg_ip_locations') }}
),

final AS (
  SELECT * FROM locations_with_keys
)


SELECT * FROM final
