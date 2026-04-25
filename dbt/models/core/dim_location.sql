{{
  config(
    materialized='table',
    schema='core',
    cluster_by=['country_name', 'region_name']
  )
}}

WITH locations_distinct AS (
  SELECT DISTINCT
    country_name,
    region_name,
    city_name,
    geo_completeness_level,
    has_geo_data,
  FROM {{ ref('stg_ip_locations') }}
  WHERE has_geo_data = TRUE
),

locations_with_keys AS (
  SELECT
    -- Surrogate key
    ROW_NUMBER() OVER (ORDER BY country_name, region_name, city_name) AS location_key,

    -- Natural key (composite: country, region, city)
    country_name,
    region_name,
    city_name,

    -- Quality indicators
    geo_completeness_level,
    has_geo_data,

  FROM locations_distinct
),

final AS (
  SELECT * FROM locations_with_keys
)


SELECT * FROM final
