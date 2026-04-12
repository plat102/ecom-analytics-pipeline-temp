-- Data Profiling Query for glamira_raw.products
-- Purpose: Statistical analysis of product catalog data

-- Latest partition date: 2026-04-10 (18,938 rows)


-- =============================================================================
-- SECTION 0: Verify Latest Partition Date (RUN THIS FIRST)
-- =============================================================================

SELECT
    'Partition Verification' AS check_type,
    DATE(ingested_at) AS partition_date,
    COUNT(*) AS row_count,
    COUNT(DISTINCT product_id) AS distinct_products,
    COUNTIF(status = 'success') AS successful_crawls,
    ROUND(COUNTIF(status = 'success') / COUNT(*) * 100, 2) AS success_rate_pct
FROM `ecom-analytics-tp.glamira_raw.products`
GROUP BY partition_date
ORDER BY partition_date DESC
LIMIT 3;

-- Expected result: Latest partition should be 2026-04-10 with 18,938 rows
-- If different, update all queries below with the correct partition date


-- =============================================================================
-- SECTION 1: Basic Statistics
-- Report Section: Executive Summary + 1.1 Statistical Analysis - Core Identifiers
-- =============================================================================

SELECT
    'Basic Statistics' AS metric_category,

    -- Row counts
    COUNT(*) AS total_rows,
    COUNT(DISTINCT product_id) AS distinct_product_ids,
    ROUND(COUNT(DISTINCT product_id) / COUNT(*) * 100, 2) AS product_id_uniqueness_pct,

    -- Ingestion metadata
    MIN(DATE(ingested_at)) AS earliest_ingestion_date,
    MAX(DATE(ingested_at)) AS latest_ingestion_date,
    DATE_DIFF(MAX(DATE(ingested_at)), MIN(DATE(ingested_at)), DAY) AS ingestion_period_days

FROM `ecom-analytics-tp.glamira_raw.products`
WHERE DATE(ingested_at) = '2026-04-10';


-- =============================================================================
-- SECTION 2: Column-Level Null Rate Analysis
-- Report Section: 1.1 Statistical Analysis - Core Identifiers & Business Dimensions
-- =============================================================================

SELECT
    'Null Rates' AS metric_category,

    COUNT(*) AS total_rows,

    -- Core identifiers (should be 100% populated)
    COUNTIF(product_id IS NULL OR product_id = '') AS null_product_id_count,
    ROUND(COUNTIF(product_id IS NULL OR product_id = '') / COUNT(*) * 100, 2) AS null_product_id_pct,

    COUNTIF(url IS NULL OR url = '') AS null_url_count,
    ROUND(COUNTIF(url IS NULL OR url = '') / COUNT(*) * 100, 2) AS null_url_pct,

    -- Crawl status fields
    COUNTIF(status IS NULL OR status = '') AS null_status_count,
    ROUND(COUNTIF(status IS NULL OR status = '') / COUNT(*) * 100, 2) AS null_status_pct,

    COUNTIF(http_status IS NULL) AS null_http_status_count,
    ROUND(COUNTIF(http_status IS NULL) / COUNT(*) * 100, 2) AS null_http_status_pct,

    -- Product details
    COUNTIF(product_name IS NULL OR product_name = '') AS null_product_name_count,
    ROUND(COUNTIF(product_name IS NULL OR product_name = '') / COUNT(*) * 100, 2) AS null_product_name_pct,

    COUNTIF(sku IS NULL OR sku = '') AS null_sku_count,
    ROUND(COUNTIF(sku IS NULL OR sku = '') / COUNT(*) * 100, 2) AS null_sku_pct,

    -- Category fields
    COUNTIF(category IS NULL OR category = '') AS null_category_count,
    ROUND(COUNTIF(category IS NULL OR category = '') / COUNT(*) * 100, 2) AS null_category_pct,

    COUNTIF(category_name IS NULL OR category_name = '') AS null_category_name_count,
    ROUND(COUNTIF(category_name IS NULL OR category_name = '') / COUNT(*) * 100, 2) AS null_category_name_pct,

    -- Store and currency
    COUNTIF(store_code IS NULL OR store_code = '') AS null_store_code_count,
    ROUND(COUNTIF(store_code IS NULL OR store_code = '') / COUNT(*) * 100, 2) AS null_store_code_pct,

    COUNTIF(currency_code IS NULL OR currency_code = '') AS null_currency_code_count,
    ROUND(COUNTIF(currency_code IS NULL OR currency_code = '') / COUNT(*) * 100, 2) AS null_currency_code_pct,

    -- Price fields
    COUNTIF(price IS NULL OR price = '') AS null_price_count,
    ROUND(COUNTIF(price IS NULL OR price = '') / COUNT(*) * 100, 2) AS null_price_pct,

    COUNTIF(min_price IS NULL OR min_price = '') AS null_min_price_count,
    ROUND(COUNTIF(min_price IS NULL OR min_price = '') / COUNT(*) * 100, 2) AS null_min_price_pct,

    COUNTIF(max_price IS NULL OR max_price = '') AS null_max_price_count,
    ROUND(COUNTIF(max_price IS NULL OR max_price = '') / COUNT(*) * 100, 2) AS null_max_price_pct

FROM `ecom-analytics-tp.glamira_raw.products`
WHERE DATE(ingested_at) = '2026-04-10';


-- =============================================================================
-- SECTION 3: Distinct Counts and Cardinality
-- Report Section: 1.1 Statistical Analysis - Business Dimensions
-- =============================================================================

SELECT
    'Cardinality' AS metric_category,

    -- Product dimensions
    COUNT(DISTINCT product_id) AS distinct_product_ids,
    COUNT(DISTINCT url) AS distinct_urls,
    COUNT(DISTINCT sku) AS distinct_skus,

    -- Catalog structure
    COUNT(DISTINCT category) AS distinct_categories,
    COUNT(DISTINCT category_name) AS distinct_category_names,
    COUNT(DISTINCT collection) AS distinct_collections,
    COUNT(DISTINCT attribute_set) AS distinct_attribute_sets,

    -- Store and currency
    COUNT(DISTINCT store_code) AS distinct_stores,
    COUNT(DISTINCT currency_code) AS distinct_currencies,

    -- Product types and attributes
    COUNT(DISTINCT product_type) AS distinct_product_types,
    COUNT(DISTINCT gender) AS distinct_genders,
    COUNT(DISTINCT material_design) AS distinct_material_designs

FROM `ecom-analytics-tp.glamira_raw.products`
WHERE DATE(ingested_at) = '2026-04-10';


-- =============================================================================
-- SECTION 4: Crawl Status Distribution
-- Report Section: 1.1 Statistical Analysis - Business Dimensions (Top 3 Values)
-- =============================================================================

SELECT
    'Crawl Status Distribution' AS metric_category,
    status,
    COUNT(*) AS product_count,
    ROUND(COUNT(*) / SUM(COUNT(*)) OVER() * 100, 2) AS percentage

FROM `ecom-analytics-tp.glamira_raw.products`
WHERE DATE(ingested_at) = '2026-04-10'
GROUP BY status
ORDER BY product_count DESC;


-- =============================================================================
-- SECTION 5: HTTP Status Code Distribution
-- Report Section: 1.1 Statistical Analysis - Business Dimensions
-- =============================================================================

SELECT
    'HTTP Status Code Distribution' AS metric_category,
    http_status,
    COUNT(*) AS product_count,
    ROUND(COUNT(*) / SUM(COUNT(*)) OVER() * 100, 2) AS percentage

FROM `ecom-analytics-tp.glamira_raw.products`
WHERE DATE(ingested_at) = '2026-04-10'
      AND http_status IS NOT NULL
GROUP BY http_status
ORDER BY product_count DESC;


-- =============================================================================
-- SECTION 6: Category Distribution
-- Report Section: 1.1 Statistical Analysis - Business Dimensions (Top 3 Values)
-- =============================================================================

SELECT
    'Category Distribution' AS metric_category,
    category,
    category_name,
    COUNT(*) AS product_count,
    ROUND(COUNT(*) / SUM(COUNT(*)) OVER() * 100, 2) AS percentage,
    COUNT(DISTINCT store_code) AS stores_with_category

FROM `ecom-analytics-tp.glamira_raw.products`
WHERE DATE(ingested_at) = '2026-04-10'
      AND category IS NOT NULL
GROUP BY category, category_name
ORDER BY product_count DESC
LIMIT 20;


-- =============================================================================
-- SECTION 7: Store Distribution
-- Report Section: 1.1 Statistical Analysis - Business Dimensions (Top 3 Values)
-- =============================================================================

SELECT
    'Store Distribution' AS metric_category,
    store_code,
    COUNT(*) AS product_count,
    ROUND(COUNT(*) / SUM(COUNT(*)) OVER() * 100, 2) AS percentage,
    COUNT(DISTINCT category) AS categories_in_store

FROM `ecom-analytics-tp.glamira_raw.products`
WHERE DATE(ingested_at) = '2026-04-10'
      AND store_code IS NOT NULL
GROUP BY store_code
ORDER BY product_count DESC
LIMIT 20;


-- =============================================================================
-- SECTION 8: Price Statistics by Currency
-- Report Section: 1.1 Statistical Analysis - Numeric Measures
-- =============================================================================

SELECT
    'Price Statistics by Currency' AS metric_category,
    currency_code,

    COUNT(*) AS products_with_currency,
    COUNTIF(price IS NOT NULL AND price != '') AS products_with_price,

    -- Price statistics (cast STRING to FLOAT64)
    ROUND(AVG(SAFE_CAST(price AS FLOAT64)), 2) AS avg_price,
    APPROX_QUANTILES(SAFE_CAST(price AS FLOAT64), 100)[OFFSET(50)] AS median_price,
    MIN(SAFE_CAST(price AS FLOAT64)) AS min_price,
    MAX(SAFE_CAST(price AS FLOAT64)) AS max_price,

    -- Price range distribution
    APPROX_QUANTILES(SAFE_CAST(price AS FLOAT64), 100)[OFFSET(25)] AS p25_price,
    APPROX_QUANTILES(SAFE_CAST(price AS FLOAT64), 100)[OFFSET(75)] AS p75_price,

    -- Min/Max price analysis
    ROUND(AVG(SAFE_CAST(min_price AS FLOAT64)), 2) AS avg_min_price,
    ROUND(AVG(SAFE_CAST(max_price AS FLOAT64)), 2) AS avg_max_price

FROM `ecom-analytics-tp.glamira_raw.products`
WHERE DATE(ingested_at) = '2026-04-10'
      AND currency_code IS NOT NULL
GROUP BY currency_code
ORDER BY products_with_currency DESC;


-- =============================================================================
-- SECTION 9: String Length Patterns
-- Report Section: 1.2 Structural Analysis - String Length Patterns
-- =============================================================================

SELECT
    'String Length Patterns' AS metric_category,

    -- Product name length
    ROUND(AVG(LENGTH(product_name)), 2) AS avg_product_name_length,
    MIN(LENGTH(product_name)) AS min_product_name_length,
    MAX(LENGTH(product_name)) AS max_product_name_length,

    -- URL length
    ROUND(AVG(LENGTH(url)), 2) AS avg_url_length,
    MIN(LENGTH(url)) AS min_url_length,
    MAX(LENGTH(url)) AS max_url_length,

    -- SKU length
    ROUND(AVG(LENGTH(sku)), 2) AS avg_sku_length,
    MIN(LENGTH(sku)) AS min_sku_length,
    MAX(LENGTH(sku)) AS max_sku_length,

    -- Category name length
    ROUND(AVG(LENGTH(category_name)), 2) AS avg_category_name_length,
    MIN(LENGTH(category_name)) AS min_category_name_length,
    MAX(LENGTH(category_name)) AS max_category_name_length

FROM `ecom-analytics-tp.glamira_raw.products`
WHERE DATE(ingested_at) = '2026-04-10';


-- =============================================================================
-- SECTION 10: URL Format Analysis
-- Report Section: 1.2 Structural Analysis - Format Conformance
-- =============================================================================

SELECT
    'URL Format Analysis' AS metric_category,

    COUNT(*) AS total_rows,

    -- Product URLs
    COUNTIF(url LIKE 'http%') AS urls_with_http,
    COUNTIF(url LIKE 'https%') AS urls_with_https,
    ROUND(COUNTIF(url LIKE 'https%') / COUNT(*) * 100, 2) AS https_pct,

    -- Image URLs
    COUNTIF(primary_image_url IS NOT NULL AND primary_image_url != '') AS products_with_image,
    COUNTIF(primary_image_url LIKE 'https%') AS images_with_https,

    -- Video URLs
    COUNTIF(primary_video_url IS NOT NULL AND primary_video_url != '') AS products_with_video,

    -- URL completeness
    COUNTIF(url IS NOT NULL AND url != ''
            AND primary_image_url IS NOT NULL AND primary_image_url != '') AS products_with_url_and_image

FROM `ecom-analytics-tp.glamira_raw.products`
WHERE DATE(ingested_at) = '2026-04-10';


-- =============================================================================
-- SECTION 11: Product Type Distribution
-- Report Section: 1.2 Structural Analysis - Value Distribution Patterns
-- =============================================================================

SELECT
    'Product Type Distribution' AS metric_category,
    product_type,
    COUNT(*) AS product_count,
    ROUND(COUNT(*) / SUM(COUNT(*)) OVER() * 100, 2) AS percentage

FROM `ecom-analytics-tp.glamira_raw.products`
WHERE DATE(ingested_at) = '2026-04-10'
      AND product_type IS NOT NULL
GROUP BY product_type
ORDER BY product_count DESC;


-- =============================================================================
-- SECTION 12: Gender Distribution
-- Report Section: 1.2 Structural Analysis - Value Distribution Patterns
-- =============================================================================

SELECT
    'Gender Distribution' AS metric_category,
    gender,
    COUNT(*) AS product_count,
    ROUND(COUNT(*) / SUM(COUNT(*)) OVER() * 100, 2) AS percentage

FROM `ecom-analytics-tp.glamira_raw.products`
WHERE DATE(ingested_at) = '2026-04-10'
      AND gender IS NOT NULL
GROUP BY gender
ORDER BY product_count DESC;


-- =============================================================================
-- SECTION 13: Data Quality Issues Detection
-- Report Section: 1.3 Data Quality Issues
-- =============================================================================

SELECT
    'Data Quality Issues' AS metric_category,

    COUNT(*) AS total_rows,

    -- CRITICAL: Missing primary key
    COUNTIF(product_id IS NULL OR product_id = '') AS missing_product_id,

    -- CRITICAL: Duplicate product IDs
    (COUNT(*) - COUNT(DISTINCT product_id)) AS duplicate_product_ids,
    ROUND((COUNT(*) - COUNT(DISTINCT product_id)) / COUNT(*) * 100, 2) AS duplicate_pct,

    -- HIGH: Failed crawls
    COUNTIF(status != 'success') AS failed_crawls,
    ROUND(COUNTIF(status != 'success') / COUNT(*) * 100, 2) AS failed_crawl_pct,

    -- HIGH: Products missing critical fields
    COUNTIF(product_id IS NULL OR product_id = ''
            OR url IS NULL OR url = '') AS products_missing_identifiers,

    -- HIGH: Products with invalid prices
    COUNTIF(price IS NOT NULL AND price != ''
            AND SAFE_CAST(price AS FLOAT64) <= 0) AS invalid_prices,

    -- MEDIUM: Products with price but no currency
    COUNTIF((price IS NOT NULL AND price != '')
            AND (currency_code IS NULL OR currency_code = '')) AS price_without_currency,

    -- MEDIUM: Products with empty or very short names
    COUNTIF(product_name IS NOT NULL AND product_name != ''
            AND LENGTH(product_name) < 3) AS very_short_names,

    -- MEDIUM: Products with no category
    COUNTIF(category IS NULL OR category = '') AS no_category,

    -- LOW: Products missing images
    COUNTIF(primary_image_url IS NULL OR primary_image_url = '') AS no_image

FROM `ecom-analytics-tp.glamira_raw.products`
WHERE DATE(ingested_at) = '2026-04-10';


-- =============================================================================
-- SECTION 14: Table-Level Primary Key Analysis
-- Report Section: 2.1 Primary Key Analysis
-- =============================================================================

SELECT
    'Primary Key Analysis (product_id)' AS metric_category,

    COUNT(*) AS total_rows,
    COUNT(DISTINCT product_id) AS distinct_product_ids,

    -- Duplicate detection
    COUNT(*) - COUNT(DISTINCT product_id) AS duplicate_product_ids,
    ROUND((COUNT(*) - COUNT(DISTINCT product_id)) / COUNT(*) * 100, 2) AS duplicate_pct,

    -- Null detection
    COUNTIF(product_id IS NULL OR product_id = '') AS null_product_ids,
    ROUND(COUNTIF(product_id IS NULL OR product_id = '') / COUNT(*) * 100, 2) AS null_pct,

    -- Uniqueness rate
    ROUND(COUNT(DISTINCT product_id) / COUNT(*) * 100, 2) AS uniqueness_rate

FROM `ecom-analytics-tp.glamira_raw.products`
WHERE DATE(ingested_at) = '2026-04-10';


-- =============================================================================
-- SECTION 15: Top Duplicated Product IDs (if any)
-- Report Section: 2.1 Primary Key Analysis
-- =============================================================================

SELECT
    'Top Duplicated Product IDs' AS metric_category,
    product_id,
    COUNT(*) AS occurrence_count,
    STRING_AGG(DISTINCT store_code, ', ' LIMIT 3) AS stores,
    STRING_AGG(DISTINCT category_name, ', ' LIMIT 3) AS categories

FROM `ecom-analytics-tp.glamira_raw.products`
WHERE DATE(ingested_at) = '2026-04-10'
      AND product_id IS NOT NULL
      AND product_id != ''
GROUP BY product_id
HAVING COUNT(*) > 1
ORDER BY occurrence_count DESC
LIMIT 20;


-- =============================================================================
-- SECTION 16: Business Rules Validation - Price Consistency
-- Report Section: 2.3 Business Rules Validation
-- =============================================================================

SELECT
    'Price Consistency Validation' AS metric_category,

    COUNT(*) AS total_rows,

    -- Rule 1: min_price <= price <= max_price
    COUNTIF(SAFE_CAST(min_price AS FLOAT64) IS NOT NULL
            AND SAFE_CAST(price AS FLOAT64) IS NOT NULL
            AND SAFE_CAST(max_price AS FLOAT64) IS NOT NULL) AS records_with_all_prices,

    COUNTIF(SAFE_CAST(min_price AS FLOAT64) > SAFE_CAST(price AS FLOAT64)) AS min_price_exceeds_price,

    COUNTIF(SAFE_CAST(price AS FLOAT64) > SAFE_CAST(max_price AS FLOAT64)) AS price_exceeds_max_price,

    COUNTIF(SAFE_CAST(min_price AS FLOAT64) > SAFE_CAST(max_price AS FLOAT64)) AS min_exceeds_max,

    -- Rule 2: Successful crawls should have product name
    COUNTIF(status = 'success'
            AND (product_name IS NULL OR product_name = '')) AS success_but_no_name,

    -- Rule 3: Successful crawls should have price
    COUNTIF(status = 'success'
            AND (price IS NULL OR price = '')) AS success_but_no_price

FROM `ecom-analytics-tp.glamira_raw.products`
WHERE DATE(ingested_at) = '2026-04-10';


-- =============================================================================
-- SECTION 17: Cross-Column Relationships - Status vs Data Completeness
-- Report Section: 2.4 Cross-Column Relationships
-- =============================================================================

SELECT
    'Status vs Data Completeness' AS metric_category,
    status,

    COUNT(*) AS total_products,

    -- Data completeness by status
    COUNTIF(product_name IS NOT NULL AND product_name != '') AS has_product_name,
    ROUND(COUNTIF(product_name IS NOT NULL AND product_name != '') / COUNT(*) * 100, 2) AS name_pct,

    COUNTIF(price IS NOT NULL AND price != '') AS has_price,
    ROUND(COUNTIF(price IS NOT NULL AND price != '') / COUNT(*) * 100, 2) AS price_pct,

    COUNTIF(category IS NOT NULL AND category != '') AS has_category,
    ROUND(COUNTIF(category IS NOT NULL AND category != '') / COUNT(*) * 100, 2) AS category_pct,

    COUNTIF(primary_image_url IS NOT NULL AND primary_image_url != '') AS has_image,
    ROUND(COUNTIF(primary_image_url IS NOT NULL AND primary_image_url != '') / COUNT(*) * 100, 2) AS image_pct

FROM `ecom-analytics-tp.glamira_raw.products`
WHERE DATE(ingested_at) = '2026-04-10'
GROUP BY status
ORDER BY total_products DESC;


-- =============================================================================
-- SECTION 18: Cross-Column Relationships - Category vs Store
-- Report Section: 2.4 Cross-Column Relationships
-- =============================================================================

SELECT
    'Category vs Store Cross-Tab' AS metric_category,
    category_name,
    COUNT(DISTINCT store_code) AS stores_with_category,
    COUNT(*) AS total_products,
    STRING_AGG(DISTINCT store_code, ', ' LIMIT 5) AS sample_stores

FROM `ecom-analytics-tp.glamira_raw.products`
WHERE DATE(ingested_at) = '2026-04-10'
      AND category_name IS NOT NULL
      AND store_code IS NOT NULL
GROUP BY category_name
ORDER BY total_products DESC
LIMIT 20;


-- =============================================================================
-- SECTION 19: Sample Records for Validation - Successful Crawls
-- Report Section: Appendix
-- =============================================================================

SELECT
    'Sample Successful Products' AS metric_category,
    product_id,
    product_name,
    category_name,
    store_code,
    price,
    currency_code,
    status

FROM `ecom-analytics-tp.glamira_raw.products`
WHERE DATE(ingested_at) = '2026-04-10'
      AND status = 'success'
LIMIT 10;


-- =============================================================================
-- SECTION 20: Sample Records for Validation - Failed Crawls
-- Report Section: Appendix
-- =============================================================================

SELECT
    'Sample Failed Products' AS metric_category,
    product_id,
    url,
    status,
    http_status,
    error_message

FROM `ecom-analytics-tp.glamira_raw.products`
WHERE DATE(ingested_at) = '2026-04-10'
      AND status != 'success'
LIMIT 10;
