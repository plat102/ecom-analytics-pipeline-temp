{{
  config(
    materialized='table',
    schema='core',
    cluster_by=['country_name', 'region_name']
  )
}}

WITH stg_ip_location__get_distinct AS (
  SELECT DISTINCT
    country_name,
    region_name,
    city_name,
    geo_completeness_level
  FROM {{ ref('stg_ip_locations') }}
  WHERE country_name IS NOT NULL
),

stg_ip_location__add_surrogate_key AS (
  SELECT
    -- Surrogate key
    ROW_NUMBER() OVER (ORDER BY country_name, region_name, city_name) AS location_key,

    -- Natural key (composite: country, region, city)
    country_name,
    region_name,
    city_name,

    -- Quality indicator
    geo_completeness_level

  FROM stg_ip_location__get_distinct
),

final AS (
  SELECT * FROM stg_ip_location__add_surrogate_key
)


SELECT * FROM final
