-- Data Profiling Query for glamira_raw.events
-- Purpose: Comprehensive statistical analysis of events table

-- IMPORTANT: Partition date is 2026-04-09

-- =============================================================================
-- EXECUTION NOTES
-- =============================================================================

-- Total rows: 41,432,473
-- Table size: 19.51 GB
-- Total estimated cost: ~$0.10-0.20 (with partition pruning)

-- To run all sections:
-- 1. Copy each section individually to BigQuery Console
-- 2. Execute and save results
-- 3. Fill corresponding sections in events_profile_report.md

-- Cost optimization:
-- - All queries use partition pruning: WHERE DATE(ingested_at) = '<date>'
-- - APPROX functions used where appropriate
-- - LIMIT clauses on distribution queries


-- =============================================================================
-- SECTION 0: Verify Latest Partition Date (RUN THIS FIRST)
-- =============================================================================

SELECT
    'Partition Verification' AS check_type,
    DATE(ingested_at) AS partition_date,
    COUNT(*) AS row_count,
    ROUND(SUM(CASE WHEN _id IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS data_quality_pct
FROM `ecom-analytics-tp.glamira_raw.events`
GROUP BY partition_date
ORDER BY partition_date DESC
LIMIT 3;

-- Expected result: Latest partition should be 2026-04-09 with 41,432,473 rows
-- If different, update all queries below with the correct partition date


-- =============================================================================
-- SECTION 1: Basic Statistics (for Report Section 1.1 - Statistical Analysis)
-- =============================================================================

SELECT
    'Basic Statistics' AS metric_category,

    -- Row counts
    COUNT(*) AS total_rows,
    COUNT(DISTINCT _id) AS distinct_event_ids,

    -- Uniqueness check
    ROUND(COUNT(DISTINCT _id) / COUNT(*) * 100, 2) AS event_id_uniqueness_pct,

    -- Ingestion metadata
    MIN(DATE(ingested_at)) AS earliest_ingestion_date,
    MAX(DATE(ingested_at)) AS latest_ingestion_date,
    DATE_DIFF(MAX(DATE(ingested_at)), MIN(DATE(ingested_at)), DAY) AS ingestion_period_days,

    -- Event timestamp range
    MIN(TIMESTAMP_SECONDS(time_stamp)) AS earliest_event_timestamp,
    MAX(TIMESTAMP_SECONDS(time_stamp)) AS latest_event_timestamp,
    DATE_DIFF(
        DATE(MAX(TIMESTAMP_SECONDS(time_stamp))),
        DATE(MIN(TIMESTAMP_SECONDS(time_stamp))),
        DAY
    ) AS event_period_days

FROM `ecom-analytics-tp.glamira_raw.events`
WHERE DATE(ingested_at) = '2026-04-09';  -- Partition pruning


-- =============================================================================
-- SECTION 2: Null Rate Analysis - Core Identifiers
-- (for Report Section 1.1 - Statistical Analysis - Core Identifiers table)
-- =============================================================================

SELECT
    'Core Identifiers Null Rates' AS metric_category,

    COUNT(*) AS total_rows,

    -- _id (event_id)
    COUNT(*) - COUNTIF(_id IS NULL) AS non_null_event_id,
    COUNTIF(_id IS NULL) AS null_event_id_count,
    ROUND(COUNTIF(_id IS NULL) / COUNT(*) * 100, 2) AS null_event_id_pct,

    -- collection (event type)
    COUNT(*) - COUNTIF(collection IS NULL) AS non_null_collection,
    COUNTIF(collection IS NULL) AS null_collection_count,
    ROUND(COUNTIF(collection IS NULL) / COUNT(*) * 100, 2) AS null_collection_pct,

    -- time_stamp
    COUNT(*) - COUNTIF(time_stamp IS NULL) AS non_null_timestamp,
    COUNTIF(time_stamp IS NULL) AS null_timestamp_count,
    ROUND(COUNTIF(time_stamp IS NULL) / COUNT(*) * 100, 2) AS null_timestamp_pct,

    -- Cardinality
    COUNT(DISTINCT _id) AS distinct_event_ids,
    COUNT(DISTINCT collection) AS distinct_event_types

FROM `ecom-analytics-tp.glamira_raw.events`
WHERE DATE(ingested_at) = '2026-04-09';


-- =============================================================================
-- SECTION 3: Null Rate Analysis - Business Dimensions
-- (for Report Section 1.1 - Statistical Analysis - Business Dimensions table)
-- =============================================================================

SELECT
    'Business Dimensions Null Rates' AS metric_category,

    COUNT(*) AS total_rows,

    -- store_id
    COUNT(*) - COUNTIF(store_id IS NULL) AS non_null_store_id,
    COUNTIF(store_id IS NULL) AS null_store_id_count,
    ROUND(COUNTIF(store_id IS NULL) / COUNT(*) * 100, 2) AS null_store_id_pct,
    COUNT(DISTINCT store_id) AS distinct_stores,

    -- product_id
    COUNT(*) - COUNTIF(product_id IS NULL) AS non_null_product_id,
    COUNTIF(product_id IS NULL) AS null_product_id_count,
    ROUND(COUNTIF(product_id IS NULL) / COUNT(*) * 100, 2) AS null_product_id_pct,
    COUNT(DISTINCT product_id) AS distinct_products,

    -- device_id (used as session_id)
    COUNT(*) - COUNTIF(device_id IS NULL) AS non_null_session_id,
    COUNTIF(device_id IS NULL) AS null_session_id_count,
    ROUND(COUNTIF(device_id IS NULL) / COUNT(*) * 100, 2) AS null_session_id_pct,
    COUNT(DISTINCT device_id) AS distinct_sessions,

    -- user_id_db
    COUNT(*) - COUNTIF(user_id_db IS NULL OR user_id_db = '') AS non_null_user_id,
    COUNTIF(user_id_db IS NULL OR user_id_db = '') AS null_user_id_count,
    ROUND(COUNTIF(user_id_db IS NULL OR user_id_db = '') / COUNT(*) * 100, 2) AS null_user_id_pct,
    COUNT(DISTINCT user_id_db) AS distinct_users,

    -- ip
    COUNT(*) - COUNTIF(ip IS NULL) AS non_null_ip,
    COUNTIF(ip IS NULL) AS null_ip_count,
    ROUND(COUNTIF(ip IS NULL) / COUNT(*) * 100, 2) AS null_ip_pct,
    COUNT(DISTINCT ip) AS distinct_ips,

    -- currency
    COUNT(*) - COUNTIF(currency IS NULL) AS non_null_currency,
    COUNTIF(currency IS NULL) AS null_currency_count,
    ROUND(COUNTIF(currency IS NULL) / COUNT(*) * 100, 2) AS null_currency_pct,
    COUNT(DISTINCT currency) AS distinct_currencies

FROM `ecom-analytics-tp.glamira_raw.events`
WHERE DATE(ingested_at) = '2026-04-09';


-- =============================================================================
-- SECTION 4: Numeric Measures - Price Statistics
-- (for Report Section 1.1 - Statistical Analysis - Numeric Measures table)
-- =============================================================================

SELECT
    'Price Statistics' AS metric_category,

    COUNT(*) AS total_rows,

    -- price (stored as STRING, need to cast)
    COUNT(*) - COUNTIF(price IS NULL) AS non_null_price,
    COUNTIF(price IS NULL) AS null_price_count,
    ROUND(COUNTIF(price IS NULL) / COUNT(*) * 100, 2) AS null_price_pct,

    -- Price stats (where not null and can cast to FLOAT64)
    MIN(SAFE_CAST(price AS FLOAT64)) AS min_price,
    MAX(SAFE_CAST(price AS FLOAT64)) AS max_price,
    ROUND(AVG(SAFE_CAST(price AS FLOAT64)), 2) AS avg_price,
    APPROX_QUANTILES(SAFE_CAST(price AS FLOAT64), 100)[OFFSET(50)] AS median_price,
    ROUND(STDDEV(SAFE_CAST(price AS FLOAT64)), 2) AS stddev_price,

    -- Invalid prices (<=0)
    COUNTIF(SAFE_CAST(price AS FLOAT64) <= 0) AS invalid_price_count,

    -- cart_products.amount (need to unnest)
    COUNTIF(ARRAY_LENGTH(cart_products) > 0) AS events_with_cart_products

FROM `ecom-analytics-tp.glamira_raw.events`
WHERE DATE(ingested_at) = '2026-04-09';


-- =============================================================================
-- SECTION 5: Temporal Fields Analysis
-- (for Report Section 1.1 - Statistical Analysis - Temporal Fields table)
-- =============================================================================

SELECT
    'Temporal Fields' AS metric_category,

    COUNT(*) AS total_rows,

    -- time_stamp (converted to TIMESTAMP)
    COUNT(*) - COUNTIF(time_stamp IS NULL) AS non_null_timestamp,
    COUNTIF(time_stamp IS NULL) AS null_timestamp_count,
    ROUND(COUNTIF(time_stamp IS NULL) / COUNT(*) * 100, 2) AS null_timestamp_pct,

    MIN(TIMESTAMP_SECONDS(time_stamp)) AS min_timestamp,
    MAX(TIMESTAMP_SECONDS(time_stamp)) AS max_timestamp,
    DATE_DIFF(
        DATE(MAX(TIMESTAMP_SECONDS(time_stamp))),
        DATE(MIN(TIMESTAMP_SECONDS(time_stamp))),
        DAY
    ) AS timestamp_range_days,

    -- Future timestamps (data quality issue)
    COUNTIF(TIMESTAMP_SECONDS(time_stamp) > CURRENT_TIMESTAMP()) AS future_timestamps,

    -- Very old timestamps (before 2019)
    COUNTIF(TIMESTAMP_SECONDS(time_stamp) < TIMESTAMP('2019-01-01')) AS pre_2019_timestamps,

    -- ingested_at
    MIN(ingested_at) AS min_ingested_at,
    MAX(ingested_at) AS max_ingested_at

FROM `ecom-analytics-tp.glamira_raw.events`
WHERE DATE(ingested_at) = '2026-04-09';


-- =============================================================================
-- SECTION 6: Nested Fields Analysis - option and cart_products
-- (for Report Section 1.1 - Statistical Analysis - Nested/Complex Fields table)
-- =============================================================================

SELECT
    'Nested Fields Statistics' AS metric_category,

    COUNT(*) AS total_events,

    -- option array (REPEATED RECORD)
    COUNTIF(ARRAY_LENGTH(option) > 0) AS events_with_options,
    ROUND(COUNTIF(ARRAY_LENGTH(option) > 0) / COUNT(*) * 100, 2) AS events_with_options_pct,
    COUNTIF(ARRAY_LENGTH(option) = 0 OR option IS NULL) AS events_without_options,
    ROUND(COUNTIF(ARRAY_LENGTH(option) = 0 OR option IS NULL) / COUNT(*) * 100, 2) AS null_or_empty_option_pct,

    ROUND(AVG(ARRAY_LENGTH(option)), 2) AS avg_options_per_event,
    APPROX_QUANTILES(ARRAY_LENGTH(option), 100)[OFFSET(50)] AS median_options_per_event,
    MAX(ARRAY_LENGTH(option)) AS max_options_per_event,

    -- cart_products array (REPEATED RECORD)
    COUNTIF(ARRAY_LENGTH(cart_products) > 0) AS events_with_cart_data,
    ROUND(COUNTIF(ARRAY_LENGTH(cart_products) > 0) / COUNT(*) * 100, 2) AS events_with_cart_pct,
    COUNTIF(ARRAY_LENGTH(cart_products) = 0 OR cart_products IS NULL) AS events_without_cart,

    ROUND(AVG(ARRAY_LENGTH(cart_products)), 2) AS avg_cart_products_per_event,
    APPROX_QUANTILES(ARRAY_LENGTH(cart_products), 100)[OFFSET(50)] AS median_cart_products,
    MAX(ARRAY_LENGTH(cart_products)) AS max_cart_products

FROM `ecom-analytics-tp.glamira_raw.events`
WHERE DATE(ingested_at) = '2026-04-09';


-- =============================================================================
-- SECTION 7: Event Type Distribution
-- (for Report Section 1.2 - Structural Analysis - Value Distribution Patterns)
-- =============================================================================

SELECT
    'Event Type Distribution' AS metric_category,
    collection AS event_type,
    COUNT(*) AS event_count,
    ROUND(COUNT(*) / SUM(COUNT(*)) OVER() * 100, 2) AS percentage,
    COUNT(DISTINCT device_id) AS sessions_with_this_event,
    COUNT(DISTINCT CASE WHEN product_id IS NOT NULL THEN product_id END) AS products_in_this_event

FROM `ecom-analytics-tp.glamira_raw.events`
WHERE DATE(ingested_at) = '2026-04-09'
GROUP BY collection
ORDER BY event_count DESC;


-- =============================================================================
-- SECTION 8: Store Distribution (Top 20)
-- (for Report Section 1.2 - Structural Analysis - Store Distribution)
-- =============================================================================

SELECT
    'Store Distribution' AS metric_category,
    store_id,
    COUNT(*) AS event_count,
    ROUND(COUNT(*) / SUM(COUNT(*)) OVER() * 100, 2) AS percentage,
    COUNT(DISTINCT device_id) AS sessions,
    COUNT(DISTINCT CASE WHEN product_id IS NOT NULL THEN product_id END) AS products_viewed

FROM `ecom-analytics-tp.glamira_raw.events`
WHERE DATE(ingested_at) = '2026-04-09'
GROUP BY store_id
ORDER BY event_count DESC
LIMIT 20;


-- =============================================================================
-- SECTION 9: Device and Platform Distribution
-- (for Report Section 1.2 - Structural Analysis - Device & Platform Distribution)
-- =============================================================================

-- Note: These fields may not exist in schema - check before running
-- SELECT
--     'Device and Platform' AS metric_category,
--     device_type,
--     platform,
--     COUNT(*) AS event_count,
--     ROUND(COUNT(*) / SUM(COUNT(*)) OVER() * 100, 2) AS percentage,
--     COUNT(DISTINCT device_id) AS sessions
-- FROM `ecom-analytics-tp.glamira_raw.events`
-- WHERE DATE(ingested_at) = '2026-04-09'
-- GROUP BY device_type, platform
-- ORDER BY event_count DESC;


-- =============================================================================
-- SECTION 10: Currency and Price Distribution
-- (for Report Section 1.2 - Structural Analysis)
-- =============================================================================

SELECT
    'Currency and Price' AS metric_category,
    currency,
    COUNT(*) AS events_with_currency,
    COUNT(DISTINCT product_id) AS products,

    -- Price statistics (where price is not null)
    COUNTIF(price IS NOT NULL) AS events_with_price,
    ROUND(AVG(SAFE_CAST(price AS FLOAT64)), 2) AS avg_price,
    APPROX_QUANTILES(SAFE_CAST(price AS FLOAT64), 100)[OFFSET(50)] AS median_price,
    MIN(SAFE_CAST(price AS FLOAT64)) AS min_price,
    MAX(SAFE_CAST(price AS FLOAT64)) AS max_price

FROM `ecom-analytics-tp.glamira_raw.events`
WHERE DATE(ingested_at) = '2026-04-09' AND currency IS NOT NULL
GROUP BY currency
ORDER BY events_with_currency DESC;


-- =============================================================================
-- SECTION 11: Data Quality Issues Detection
-- (for Report Section 1.3 - Data Quality Issues - Issues Summary table)
-- =============================================================================

SELECT
    'Data Quality Issues' AS metric_category,

    -- Duplicate event_ids
    COUNT(*) - COUNT(DISTINCT _id) AS duplicate_event_ids,
    ROUND((COUNT(*) - COUNT(DISTINCT _id)) / COUNT(*) * 100, 4) AS duplicate_event_id_pct,

    -- Future timestamps (data quality issue)
    COUNTIF(TIMESTAMP_SECONDS(time_stamp) > CURRENT_TIMESTAMP()) AS future_timestamps,
    ROUND(COUNTIF(TIMESTAMP_SECONDS(time_stamp) > CURRENT_TIMESTAMP()) / COUNT(*) * 100, 4) AS future_timestamp_pct,

    -- Very old timestamps (before 2019-01-01)
    COUNTIF(TIMESTAMP_SECONDS(time_stamp) < TIMESTAMP('2019-01-01')) AS very_old_timestamps,
    ROUND(COUNTIF(TIMESTAMP_SECONDS(time_stamp) < TIMESTAMP('2019-01-01')) / COUNT(*) * 100, 4) AS very_old_timestamp_pct,

    -- Invalid store_id (not in expected range 1-86)
    COUNTIF(SAFE_CAST(store_id AS INT64) IS NULL OR SAFE_CAST(store_id AS INT64) < 1 OR SAFE_CAST(store_id AS INT64) > 86) AS invalid_store_ids,
    ROUND(COUNTIF(SAFE_CAST(store_id AS INT64) IS NULL OR SAFE_CAST(store_id AS INT64) < 1 OR SAFE_CAST(store_id AS INT64) > 86) / COUNT(*) * 100, 4) AS invalid_store_id_pct,

    -- Events with null critical fields (_id, collection, time_stamp)
    COUNTIF(_id IS NULL OR collection IS NULL OR time_stamp IS NULL) AS events_missing_critical_fields,
    ROUND(COUNTIF(_id IS NULL OR collection IS NULL OR time_stamp IS NULL) / COUNT(*) * 100, 4) AS missing_critical_fields_pct,

    -- Invalid prices (negative or zero prices where price should exist)
    COUNTIF(price IS NOT NULL AND SAFE_CAST(price AS FLOAT64) <= 0) AS invalid_prices,
    ROUND(COUNTIF(price IS NOT NULL AND SAFE_CAST(price AS FLOAT64) <= 0) / COUNT(*) * 100, 4) AS invalid_price_pct

FROM `ecom-analytics-tp.glamira_raw.events`
WHERE DATE(ingested_at) = '2026-04-09';


-- =============================================================================
-- SECTION 12: Bot Sessions Detection (sessions with >1000 events)
-- (for Report Section 1.3 - Data Quality Issues)
-- =============================================================================

SELECT
    'Bot Sessions' AS metric_category,
    COUNT(*) AS suspicious_large_sessions,
    MIN(events_in_session) AS min_session_size,
    MAX(events_in_session) AS max_session_size,
    ROUND(AVG(events_in_session), 2) AS avg_session_size
FROM (
    SELECT
        device_id AS session_id,
        COUNT(*) AS events_in_session
    FROM `ecom-analytics-tp.glamira_raw.events`
    WHERE DATE(ingested_at) = '2026-04-09' AND device_id IS NOT NULL
    GROUP BY device_id
    HAVING COUNT(*) > 1000  -- Sessions with more than 1000 events (potential bots)
);


-- =============================================================================
-- SECTION 13: Temporal Distribution (events per day)
-- (for Report Section 2.4 - Cross-Column Relationships - Temporal Consistency)
-- =============================================================================

SELECT
    'Temporal Distribution' AS metric_category,
    DATE(TIMESTAMP_SECONDS(time_stamp)) AS event_date,
    COUNT(*) AS event_count,
    COUNT(DISTINCT device_id) AS sessions,
    COUNT(DISTINCT user_id_db) AS active_users,
    COUNT(DISTINCT ip) AS unique_ips

FROM `ecom-analytics-tp.glamira_raw.events`
WHERE DATE(ingested_at) = '2026-04-09'
  AND time_stamp IS NOT NULL
GROUP BY event_date
ORDER BY event_date;


-- =============================================================================
-- SECTION 14: Primary Key Validation
-- (for Report Section 2.1 - Primary Key Analysis)
-- =============================================================================

SELECT
    'Primary Key Validation' AS metric_category,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT _id) AS distinct_event_ids,
    COUNT(*) - COUNT(DISTINCT _id) AS duplicate_count,
    ROUND(COUNT(DISTINCT _id) / COUNT(*) * 100, 2) AS uniqueness_rate,
    COUNTIF(_id IS NULL) AS null_event_ids

FROM `ecom-analytics-tp.glamira_raw.events`
WHERE DATE(ingested_at) = '2026-04-09';


-- =============================================================================
-- SECTION 15: Top Duplicate Event IDs (if duplicates exist)
-- (for Report Section 2.1 - Primary Key Analysis - Duplicate Analysis)
-- =============================================================================

SELECT
    'Top Duplicate Event IDs' AS metric_category,
    _id AS event_id,
    COUNT(*) AS occurrences,
    MIN(TIMESTAMP_SECONDS(time_stamp)) AS first_occurrence,
    MAX(TIMESTAMP_SECONDS(time_stamp)) AS last_occurrence

FROM `ecom-analytics-tp.glamira_raw.events`
WHERE DATE(ingested_at) = '2026-04-09'
GROUP BY _id
HAVING COUNT(*) > 1
ORDER BY occurrences DESC
LIMIT 10;


-- =============================================================================
-- SECTION 16: Business Rule - Events with price must have currency
-- (for Report Section 2.3 - Business Rules Validation - Rule 1)
-- =============================================================================

SELECT
    'Price-Currency Rule' AS metric_category,
    COUNTIF(price IS NOT NULL) AS events_with_price,
    COUNTIF(price IS NOT NULL AND currency IS NOT NULL) AS events_with_both,
    COUNTIF(price IS NOT NULL AND currency IS NULL) AS violation_count,
    ROUND(COUNTIF(price IS NOT NULL AND currency IS NULL) / COUNTIF(price IS NOT NULL) * 100, 2) AS violation_pct,
    ROUND(COUNTIF(price IS NOT NULL AND currency IS NOT NULL) / COUNTIF(price IS NOT NULL) * 100, 2) AS compliance_rate

FROM `ecom-analytics-tp.glamira_raw.events`
WHERE DATE(ingested_at) = '2026-04-09';


-- =============================================================================
-- SECTION 17: Cross-Column - collection vs product_id presence
-- (for Report Section 2.4 - Cross-Column Relationships - Relationship 1)
-- =============================================================================

SELECT
    'Event Type vs Product ID' AS metric_category,
    collection AS event_type,
    COUNT(*) AS total_events,
    COUNTIF(product_id IS NOT NULL) AS events_with_product_id,
    COUNTIF(product_id IS NULL) AS events_with_null_product_id,
    ROUND(COUNTIF(product_id IS NULL) / COUNT(*) * 100, 2) AS product_id_null_pct

FROM `ecom-analytics-tp.glamira_raw.events`
WHERE DATE(ingested_at) = '2026-04-09'
GROUP BY collection
ORDER BY total_events DESC
LIMIT 10;


-- =============================================================================
-- SECTION 18: Cross-Column - collection vs price presence
-- (for Report Section 2.4 - Cross-Column Relationships - Relationship 2)
-- =============================================================================

SELECT
    'Event Type vs Price' AS metric_category,
    collection AS event_type,
    COUNT(*) AS total_events,
    COUNTIF(price IS NOT NULL) AS events_with_price,
    COUNTIF(price IS NULL) AS events_with_null_price,
    ROUND(COUNTIF(price IS NULL) / COUNT(*) * 100, 2) AS price_null_pct

FROM `ecom-analytics-tp.glamira_raw.events`
WHERE DATE(ingested_at) = '2026-04-09'
GROUP BY collection
ORDER BY total_events DESC
LIMIT 10;
