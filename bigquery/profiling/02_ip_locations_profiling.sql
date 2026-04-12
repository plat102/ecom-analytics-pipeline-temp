-- Data Profiling Query for glamira_raw.ip_locations
-- Purpose: Statistical analysis of IP geolocation lookup data

-- NOTE: This table contains IP addresses with geographic lookup data
-- Schema: ip (STRING), country, region, city, ingested_at
-- Current data shows IPs with mostly empty geographic fields

-- Latest partition date: 2026-04-10 (3.2M rows)


-- =============================================================================
-- SECTION 0: Verify Latest Partition Date (RUN THIS FIRST)
-- =============================================================================

SELECT
    'Partition Verification' AS check_type,
    DATE(ingested_at) AS partition_date,
    COUNT(*) AS row_count,
    COUNT(DISTINCT ip) AS distinct_ips,
    ROUND(COUNTIF(country IS NOT NULL AND country != '') / COUNT(*) * 100, 2) AS geo_data_pct
FROM `ecom-analytics-tp.glamira_raw.ip_locations`
GROUP BY partition_date
ORDER BY partition_date DESC
LIMIT 3;

-- Expected result: Latest partition should be 2026-04-10 with 3,239,628 rows
-- If different, update all queries below with the correct partition date


-- =============================================================================
-- SECTION 1: Basic Statistics
-- Report Section: Executive Summary + 1.1 Statistical Analysis - Core Identifiers
-- =============================================================================

SELECT
    'Basic Statistics' AS metric_category,

    -- Row counts
    COUNT(*) AS total_rows,
    COUNT(DISTINCT ip) AS distinct_ips,
    ROUND(COUNT(DISTINCT ip) / COUNT(*) * 100, 2) AS ip_uniqueness_pct,

    -- Ingestion metadata
    MIN(DATE(ingested_at)) AS earliest_ingestion_date,
    MAX(DATE(ingested_at)) AS latest_ingestion_date,
    DATE_DIFF(MAX(DATE(ingested_at)), MIN(DATE(ingested_at)), DAY) AS ingestion_period_days

FROM `ecom-analytics-tp.glamira_raw.ip_locations`
WHERE DATE(ingested_at) = '2026-04-10';


-- =============================================================================
-- SECTION 2: Column-Level Null Rate Analysis
-- Report Section: 1.1 Statistical Analysis - Business Dimensions
-- =============================================================================

SELECT
    'Null Rates' AS metric_category,

    COUNT(*) AS total_rows,

    -- IP field (should be 100% populated as primary identifier)
    COUNTIF(ip IS NULL OR ip = '') AS null_ip_count,
    ROUND(COUNTIF(ip IS NULL OR ip = '') / COUNT(*) * 100, 2) AS null_ip_pct,

    -- Geographic fields null rates
    COUNTIF(country IS NULL OR country = '') AS null_country_count,
    ROUND(COUNTIF(country IS NULL OR country = '') / COUNT(*) * 100, 2) AS null_country_pct,

    COUNTIF(region IS NULL OR region = '') AS null_region_count,
    ROUND(COUNTIF(region IS NULL OR region = '') / COUNT(*) * 100, 2) AS null_region_pct,

    COUNTIF(city IS NULL OR city = '') AS null_city_count,
    ROUND(COUNTIF(city IS NULL OR city = '') / COUNT(*) * 100, 2) AS null_city_pct,

    -- Records with no geographic data at all
    COUNTIF((country IS NULL OR country = '')
            AND (region IS NULL OR region = '')
            AND (city IS NULL OR city = '')) AS no_geographic_data_count,
    ROUND(COUNTIF((country IS NULL OR country = '')
                  AND (region IS NULL OR region = '')
                  AND (city IS NULL OR city = '')) / COUNT(*) * 100, 2) AS no_geographic_data_pct

FROM `ecom-analytics-tp.glamira_raw.ip_locations`
WHERE DATE(ingested_at) = '2026-04-10';


-- =============================================================================
-- SECTION 3: Geographic Coverage Analysis
-- Report Section: 1.1 Statistical Analysis - Business Dimensions
-- =============================================================================

SELECT
    'Geographic Coverage' AS metric_category,

    -- Country-level coverage
    COUNT(DISTINCT country) AS distinct_countries,
    COUNT(DISTINCT CASE WHEN country IS NOT NULL AND country != '' THEN country END) AS distinct_non_empty_countries,

    -- Region-level coverage
    COUNT(DISTINCT region) AS distinct_regions,
    COUNT(DISTINCT CASE WHEN region IS NOT NULL AND region != '' THEN region END) AS distinct_non_empty_regions,

    -- City-level coverage
    COUNT(DISTINCT city) AS distinct_cities,
    COUNT(DISTINCT CASE WHEN city IS NOT NULL AND city != '' THEN city END) AS distinct_non_empty_cities,

    -- IPs with complete geographic data
    COUNT(DISTINCT CASE
        WHEN country IS NOT NULL AND country != ''
             AND region IS NOT NULL AND region != ''
             AND city IS NOT NULL AND city != ''
        THEN ip
    END) AS ips_with_complete_data,

    ROUND(COUNT(DISTINCT CASE
        WHEN country IS NOT NULL AND country != ''
             AND region IS NOT NULL AND region != ''
             AND city IS NOT NULL AND city != ''
        THEN ip
    END) / COUNT(DISTINCT ip) * 100, 2) AS complete_data_pct

FROM `ecom-analytics-tp.glamira_raw.ip_locations`
WHERE DATE(ingested_at) = '2026-04-10';


-- =============================================================================
-- SECTION 4: Top Countries by IP Count
-- Report Section: 1.1 Statistical Analysis - Business Dimensions (Top 3 Values)
-- =============================================================================

SELECT
    'Top Countries' AS metric_category,
    country,
    COUNT(*) AS ip_count,
    COUNT(DISTINCT ip) AS distinct_ips,
    ROUND(COUNT(*) / SUM(COUNT(*)) OVER() * 100, 2) AS percentage

FROM `ecom-analytics-tp.glamira_raw.ip_locations`
WHERE DATE(ingested_at) = '2026-04-10'
      AND country IS NOT NULL
      AND country != ''
GROUP BY country
ORDER BY ip_count DESC
LIMIT 20;


-- =============================================================================
-- SECTION 5: IP Address Format Analysis
-- Report Section: 1.2 Structural Analysis - Format Conformance
-- =============================================================================

SELECT
    'IP Format Analysis' AS metric_category,

    COUNT(*) AS total_rows,

    -- IPv4 format validation (simple pattern check)
    COUNTIF(REGEXP_CONTAINS(ip, r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$')) AS ipv4_format_count,
    ROUND(COUNTIF(REGEXP_CONTAINS(ip, r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$')) / COUNT(*) * 100, 2) AS ipv4_format_pct,

    -- IPv6 format detection (contains colon)
    COUNTIF(REGEXP_CONTAINS(ip, r':')) AS ipv6_format_count,
    ROUND(COUNTIF(REGEXP_CONTAINS(ip, r':')) / COUNT(*) * 100, 2) AS ipv6_format_pct,

    -- Invalid or unexpected formats
    COUNTIF(NOT REGEXP_CONTAINS(ip, r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$')
            AND NOT REGEXP_CONTAINS(ip, r':')) AS invalid_format_count,
    ROUND(COUNTIF(NOT REGEXP_CONTAINS(ip, r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$')
                  AND NOT REGEXP_CONTAINS(ip, r':')) / COUNT(*) * 100, 2) AS invalid_format_pct

FROM `ecom-analytics-tp.glamira_raw.ip_locations`
WHERE DATE(ingested_at) = '2026-04-10';


-- =============================================================================
-- SECTION 6: String Length Patterns
-- Report Section: 1.2 Structural Analysis - String Length Patterns
-- =============================================================================

SELECT
    'String Length Patterns' AS metric_category,

    -- IP address length
    ROUND(AVG(LENGTH(ip)), 2) AS avg_ip_length,
    MIN(LENGTH(ip)) AS min_ip_length,
    MAX(LENGTH(ip)) AS max_ip_length,

    -- Country name length (for non-null values)
    ROUND(AVG(CASE WHEN country IS NOT NULL AND country != '' THEN LENGTH(country) END), 2) AS avg_country_length,
    MIN(CASE WHEN country IS NOT NULL AND country != '' THEN LENGTH(country) END) AS min_country_length,
    MAX(CASE WHEN country IS NOT NULL AND country != '' THEN LENGTH(country) END) AS max_country_length,

    -- Region name length (for non-null values)
    ROUND(AVG(CASE WHEN region IS NOT NULL AND region != '' THEN LENGTH(region) END), 2) AS avg_region_length,
    MIN(CASE WHEN region IS NOT NULL AND region != '' THEN LENGTH(region) END) AS min_region_length,
    MAX(CASE WHEN region IS NOT NULL AND region != '' THEN LENGTH(region) END) AS max_region_length,

    -- City name length (for non-null values)
    ROUND(AVG(CASE WHEN city IS NOT NULL AND city != '' THEN LENGTH(city) END), 2) AS avg_city_length,
    MIN(CASE WHEN city IS NOT NULL AND city != '' THEN LENGTH(city) END) AS min_city_length,
    MAX(CASE WHEN city IS NOT NULL AND city != '' THEN LENGTH(city) END) AS max_city_length

FROM `ecom-analytics-tp.glamira_raw.ip_locations`
WHERE DATE(ingested_at) = '2026-04-10';


-- =============================================================================
-- SECTION 7: Data Quality Issues Detection
-- Report Section: 1.3 Data Quality Issues
-- =============================================================================

SELECT
    'Data Quality Issues' AS metric_category,

    COUNT(*) AS total_rows,

    -- CRITICAL: Missing IP addresses (primary identifier)
    COUNTIF(ip IS NULL OR ip = '') AS missing_ip_addresses,
    ROUND(COUNTIF(ip IS NULL OR ip = '') / COUNT(*) * 100, 2) AS missing_ip_pct,

    -- HIGH: Invalid IP formats
    COUNTIF(ip IS NOT NULL
            AND ip != ''
            AND NOT REGEXP_CONTAINS(ip, r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$')
            AND NOT REGEXP_CONTAINS(ip, r':')) AS invalid_ip_formats,

    -- HIGH: IPs with no geographic data at all
    COUNTIF((country IS NULL OR country = '')
            AND (region IS NULL OR region = '')
            AND (city IS NULL OR city = '')) AS ips_no_geographic_data,
    ROUND(COUNTIF((country IS NULL OR country = '')
                  AND (region IS NULL OR region = '')
                  AND (city IS NULL OR city = '')) / COUNT(*) * 100, 2) AS no_geographic_data_pct,

    -- MEDIUM: Very short or suspicious values
    COUNTIF(country IS NOT NULL AND country != '' AND LENGTH(country) <= 1) AS suspicious_country_values,
    COUNTIF(city IS NOT NULL AND city != '' AND LENGTH(city) <= 1) AS suspicious_city_values,

    -- LOW: Placeholder values
    COUNTIF(country = '-' OR city = '-' OR region = '-') AS placeholder_values

FROM `ecom-analytics-tp.glamira_raw.ip_locations`
WHERE DATE(ingested_at) = '2026-04-10';


-- =============================================================================
-- SECTION 8: Private vs Public IP Analysis
-- Report Section: 1.2 Structural Analysis - Value Distribution Patterns
-- =============================================================================

SELECT
    'IP Address Type Distribution' AS metric_category,

    COUNT(*) AS total_ips,

    -- Private IP ranges (RFC 1918)
    COUNTIF(REGEXP_CONTAINS(ip, r'^10\.')
            OR REGEXP_CONTAINS(ip, r'^172\.(1[6-9]|2[0-9]|3[0-1])\.')
            OR REGEXP_CONTAINS(ip, r'^192\.168\.')) AS private_ips,
    ROUND(COUNTIF(REGEXP_CONTAINS(ip, r'^10\.')
                  OR REGEXP_CONTAINS(ip, r'^172\.(1[6-9]|2[0-9]|3[0-1])\.')
                  OR REGEXP_CONTAINS(ip, r'^192\.168\.')) / COUNT(*) * 100, 2) AS private_ip_pct,

    -- Localhost
    COUNTIF(REGEXP_CONTAINS(ip, r'^127\.')) AS localhost_ips,

    -- Public IPs (not private, not localhost)
    COUNTIF(NOT (REGEXP_CONTAINS(ip, r'^10\.')
                 OR REGEXP_CONTAINS(ip, r'^172\.(1[6-9]|2[0-9]|3[0-1])\.')
                 OR REGEXP_CONTAINS(ip, r'^192\.168\.')
                 OR REGEXP_CONTAINS(ip, r'^127\.'))) AS public_ips,
    ROUND(COUNTIF(NOT (REGEXP_CONTAINS(ip, r'^10\.')
                       OR REGEXP_CONTAINS(ip, r'^172\.(1[6-9]|2[0-9]|3[0-1])\.')
                       OR REGEXP_CONTAINS(ip, r'^192\.168\.')
                       OR REGEXP_CONTAINS(ip, r'^127\.'))) / COUNT(*) * 100, 2) AS public_ip_pct

FROM `ecom-analytics-tp.glamira_raw.ip_locations`
WHERE DATE(ingested_at) = '2026-04-10'
      AND REGEXP_CONTAINS(ip, r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$');


-- =============================================================================
-- SECTION 9: Table-Level Primary Key Analysis
-- Report Section: 2.1 Primary Key Analysis
-- =============================================================================

SELECT
    'Primary Key Analysis (ip)' AS metric_category,

    COUNT(*) AS total_rows,
    COUNT(DISTINCT ip) AS distinct_ip_values,

    -- Duplicate detection
    COUNT(*) - COUNT(DISTINCT ip) AS duplicate_ips,
    ROUND((COUNT(*) - COUNT(DISTINCT ip)) / COUNT(*) * 100, 2) AS duplicate_pct,

    -- Null detection
    COUNTIF(ip IS NULL OR ip = '') AS null_ips,
    ROUND(COUNTIF(ip IS NULL OR ip = '') / COUNT(*) * 100, 2) AS null_pct,

    -- Uniqueness rate
    ROUND(COUNT(DISTINCT ip) / COUNT(*) * 100, 2) AS uniqueness_rate

FROM `ecom-analytics-tp.glamira_raw.ip_locations`
WHERE DATE(ingested_at) = '2026-04-10';


-- =============================================================================
-- SECTION 10: Top Duplicated IPs (if any)
-- Report Section: 2.1 Primary Key Analysis
-- =============================================================================

SELECT
    'Top Duplicated IPs' AS metric_category,
    ip,
    COUNT(*) AS occurrence_count,
    STRING_AGG(DISTINCT country, ', ' LIMIT 3) AS countries,
    STRING_AGG(DISTINCT city, ', ' LIMIT 3) AS cities

FROM `ecom-analytics-tp.glamira_raw.ip_locations`
WHERE DATE(ingested_at) = '2026-04-10'
      AND ip IS NOT NULL
      AND ip != ''
GROUP BY ip
HAVING COUNT(*) > 1
ORDER BY occurrence_count DESC
LIMIT 20;


-- =============================================================================
-- SECTION 11: Geographic Completeness Levels
-- Report Section: 2.4 Cross-Column Relationships
-- =============================================================================

SELECT
    'Geographic Completeness Levels' AS metric_category,

    -- Level 0: No geographic data
    COUNTIF((country IS NULL OR country = '')
            AND (region IS NULL OR region = '')
            AND (city IS NULL OR city = '')) AS level0_no_data,
    ROUND(COUNTIF((country IS NULL OR country = '')
                  AND (region IS NULL OR region = '')
                  AND (city IS NULL OR city = '')) / COUNT(*) * 100, 2) AS level0_pct,

    -- Level 1: Country only
    COUNTIF((country IS NOT NULL AND country != '')
            AND (region IS NULL OR region = '')
            AND (city IS NULL OR city = '')) AS level1_country_only,
    ROUND(COUNTIF((country IS NOT NULL AND country != '')
                  AND (region IS NULL OR region = '')
                  AND (city IS NULL OR city = '')) / COUNT(*) * 100, 2) AS level1_pct,

    -- Level 2: Country + Region
    COUNTIF((country IS NOT NULL AND country != '')
            AND (region IS NOT NULL AND region != '')
            AND (city IS NULL OR city = '')) AS level2_country_region,
    ROUND(COUNTIF((country IS NOT NULL AND country != '')
                  AND (region IS NOT NULL AND region != '')
                  AND (city IS NULL OR city = '')) / COUNT(*) * 100, 2) AS level2_pct,

    -- Level 3: Complete (Country + Region + City)
    COUNTIF((country IS NOT NULL AND country != '')
            AND (region IS NOT NULL AND region != '')
            AND (city IS NOT NULL AND city != '')) AS level3_complete,
    ROUND(COUNTIF((country IS NOT NULL AND country != '')
                  AND (region IS NOT NULL AND region != '')
                  AND (city IS NOT NULL AND city != '')) / COUNT(*) * 100, 2) AS level3_pct

FROM `ecom-analytics-tp.glamira_raw.ip_locations`
WHERE DATE(ingested_at) = '2026-04-10';


-- =============================================================================
-- SECTION 12: Country-Level Summary Statistics
-- Report Section: 1.1 Statistical Analysis + 2.4 Cross-Column Relationships
-- =============================================================================

SELECT
    'Country-Level Summary' AS metric_category,
    country,

    COUNT(*) AS total_ips,
    COUNT(DISTINCT ip) AS distinct_ips,

    -- Region and city coverage within each country
    COUNT(DISTINCT region) AS distinct_regions,
    COUNT(DISTINCT city) AS distinct_cities,

    -- Completeness metrics
    ROUND(COUNTIF(region IS NOT NULL AND region != '') / COUNT(*) * 100, 2) AS region_populated_pct,
    ROUND(COUNTIF(city IS NOT NULL AND city != '') / COUNT(*) * 100, 2) AS city_populated_pct

FROM `ecom-analytics-tp.glamira_raw.ip_locations`
WHERE DATE(ingested_at) = '2026-04-10'
      AND country IS NOT NULL
      AND country != ''
GROUP BY country
ORDER BY total_ips DESC
LIMIT 20;


-- =============================================================================
-- SECTION 13: Sample Records for Validation
-- Report Section: Appendix
-- =============================================================================

SELECT
    'Sample Records' AS metric_category,
    ip,
    country,
    region,
    city,
    ingested_at

FROM `ecom-analytics-tp.glamira_raw.ip_locations`
WHERE DATE(ingested_at) = '2026-04-10'
ORDER BY RAND()
LIMIT 20;


-- =============================================================================
-- SECTION 14: Sample of IPs with Complete Data
-- Report Section: Appendix
-- =============================================================================

SELECT
    'Sample IPs with Complete Data' AS metric_category,
    ip,
    country,
    region,
    city

FROM `ecom-analytics-tp.glamira_raw.ip_locations`
WHERE DATE(ingested_at) = '2026-04-10'
      AND country IS NOT NULL AND country != ''
      AND region IS NOT NULL AND region != ''
      AND city IS NOT NULL AND city != ''
LIMIT 10;


-- =============================================================================
-- SECTION 15: Sample of IPs with No Geographic Data
-- Report Section: Appendix - for investigating data quality issues
-- =============================================================================

SELECT
    'Sample IPs with No Geographic Data' AS metric_category,
    ip,
    country,
    region,
    city

FROM `ecom-analytics-tp.glamira_raw.ip_locations`
WHERE DATE(ingested_at) = '2026-04-10'
      AND (country IS NULL OR country = '')
      AND (region IS NULL OR region = '')
      AND (city IS NULL OR city = '')
LIMIT 10;
