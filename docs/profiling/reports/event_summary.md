# Events Table - Data Profiling Summary

**Dataset:** `ecom-analytics-tp.glamira_raw.events`
**Data Partition:** 2026-04-09
**Total Rows:** 41,432,473

---

## Executive Summary

**Key Findings:**
- 100% unique event_ids (zero duplicates detected)
- Zero future timestamps, zero pre-2019 dates
- 100% price-currency pairing (no violations)
- 122,894 events (0.30%) have store_id outside range 1-86
- 840 sessions detected with >1000 events (potential bot traffic)

---

## 1. Row-Level Metrics

| Metric | Value |
|--------|-------|
| Total Rows | 41,432,473 |
| Distinct event_ids | 41,432,473 (100% unique) |
| Duplicate event_ids | 0 |
| Event Period | 64 days (2020-04-01 to 2020-06-04) |
| Distinct Event Types | 27 |

---

## 2. Core Field Analysis

### Critical Fields
| Field | Non-Null Count | Null % | Distinct Values |
|-------|---------------|--------|-----------------|
| _id (event_id) | 41,432,473 | 0.00% | 41,432,473 (100% unique) |
| collection (event_type) | 41,432,473 | 0.00% | 27 |
| time_stamp | 41,432,473 | 0.00% | - |
| store_id | 41,432,473 | 0.00% | 86 |
| device_id (session_id) | 41,432,473 | 0.00% | 7,691,556 sessions |
| ip | 41,432,473 | 0.00% | 3,239,628 unique IPs |

### Business Dimensions
| Field | Non-Null Count | Null % | Distinct Values | Notes |
|-------|---------------|--------|-----------------|-------|
| product_id | 22,242,720 | 46.32% | 19,417 | Expected - non-product events have NULL |
| user_id_db | 1,992,282 | 95.19% | 31,128 | Expected - anonymous users |
| currency | 186,590 | 99.55% | 85 | Expected - only transaction events |
| price | 186,590 | 99.55% | - | Expected - only transaction events |

### Price Statistics (where price IS NOT NULL)
| Metric | Value |
|--------|-------|
| Min Price | 0.0 |
| Max Price | 665,485.0 |
| Avg Price | 531.34 |
| Median Price | 400.0 |
| Std Dev | 4,935.94 |
| Invalid Prices (<=0) | 1 (0.0% - negligible) |

### Nested Fields
| Field | Events with Data | % of Total | Avg Elements | Max Elements |
|-------|-----------------|-----------|--------------|--------------|
| option (ARRAY) | 34,300,549 | 82.79% | 1.1 | 4 |
| cart_products (ARRAY) | 364,982 | 0.88% | 0.02 | 31 |

---

## 3. Event Type Distribution (Top 10)

| Event Type | Count | % | Sessions | Products |
|-----------|-------|---|----------|----------|
| view_listing_page | 11,259,694 | 27.18% | 2,414,554 | 0 |
| view_product_detail | 10,944,427 | 26.42% | 5,656,684 | 19,417 |
| select_product_option | 8,844,342 | 21.35% | 563,646 | 16,850 |
| select_product_option_quality | 2,231,825 | 5.39% | 240,732 | 8,731 |
| view_static_page | 1,451,565 | 3.50% | 481,952 | 0 |
| view_landing_page | 1,434,230 | 3.46% | 774,176 | 0 |
| product_detail_recommendation_visible | 1,302,362 | 3.14% | 512,709 | 0 |
| view_home_page | 1,053,420 | 2.54% | 608,365 | 0 |
| listing_page_recommendation_visible | 718,048 | 1.73% | 426,617 | 0 |
| product_detail_recommendation_noticed | 490,780 | 1.18% | 210,691 | 0 |

**Pattern:** Power-law distribution - top 3 event types (view/select) account for 75% of events.

---

## 4. Store Distribution (Top 10)

| Store ID | Events | % | Sessions | Products Viewed |
|---------|--------|---|----------|----------------|
| 6 | 5,162,382 | 12.46% | 776,178 | 16,103 |
| 7 | 3,359,850 | 8.11% | 615,267 | 17,950 |
| 12 | 2,509,355 | 6.06% | 499,683 | 13,967 |
| 41 | 2,360,349 | 5.70% | 498,644 | 14,659 |
| 14 | 1,866,741 | 4.51% | 384,784 | 11,620 |
| 8 | 1,680,873 | 4.06% | 378,394 | 12,185 |
| 19 | 1,537,060 | 3.71% | 309,778 | 12,373 |
| 29 | 1,412,478 | 3.41% | 275,133 | 11,756 |
| 51 | 1,378,809 | 3.33% | 194,899 | 10,885 |
| 11 | 1,246,968 | 3.01% | 138,214 | 11,243 |

**Concentration:** Top 10 stores account for 54.4% of total events.

---

## 5. Currency Distribution (Top 10)

| Currency | Events | Avg Price | Median Price | Products |
|----------|--------|-----------|--------------|----------|
| EUR | 88,456 | 416.12 | 395.0 | 8,460 |
| GBP | 17,001 | 374.37 | 336.0 | 4,013 |
| USD | 15,460 | 461.07 | 427.0 | 4,005 |
| kr | 13,879 | 5,220.0 | 5,220.0 | 3,568 |
| CAD $ | 7,694 | 474.77 | 461.0 | 2,686 |
| AU $ | 5,925 | 525.27 | 525.0 | 2,140 |
| CHF | 5,508 | 412.37 | 376.0 | 1,865 |
| RON | 4,074 | NULL | NULL | 1,608 |
| MXN $ | 3,604 | 860.0 | 850.0 | 1,343 |
| Ft | 2,617 | NULL | NULL | 1,054 |

---

## 6. Data Quality Issues

| Issue Type | Count | % of Total | Description |
|-----------|-------|-----------|-------------|
| Duplicate event_ids | 0 | 0.00% | No duplicates detected |
| Future timestamps | 0 | 0.00% | No events with timestamp > current time |
| Pre-2019 timestamps | 0 | 0.00% | No events before 2019-01-01 |
| Missing critical fields | 0 | 0.00% | No NULL event_ids, collection, or timestamps |
| **Invalid store_ids** | **122,894** | **0.30%** | Store IDs outside range 1-86 |
| Invalid prices (<=0) | 1 | 0.00% | One product with price = 0 |
| **Bot sessions (>1000 events)** | **840 sessions** | **~0.01%** | Sessions with 1,001-20,902 events |

### Bot Session Statistics
- Suspicious sessions: 840
- Min session size: 1,001 events
- Max session size: 20,902 events
- Avg session size: 1,747.82 events

---

## 7. Business Rule Validation

### Rule 1: Events with price must have currency
- **Compliance Rate:** 100.00%
- Events with price: 186,590
- Events with both price AND currency: 186,590
- Violations: 0

### Rule 2: Product events should have product_id
- **view_product_detail:** 0.00% null (PASS - all have product_id)
- **select_product_option:** 0.00% null
- **view_listing_page:** 100.00% null (expected - listing pages don't have single product_id)

### Rule 3: Non-product events should NOT have price
- **view_listing_page:** 100.00% null price
- **view_product_detail:** 100.00% null price
- **select_product_option:** 100.00% null price
- Price appears only in transaction-related events (expected pattern)

---

## 8. Temporal Distribution (First 10 Days)

| Date | Events | Sessions | Active Users | Unique IPs | Notes |
|------|--------|----------|-------------|-----------|-------|
| 2020-04-01 | 396,652 | 90,200 | 1,080 | 44,067 | Normal |
| 2020-04-02 | 407,243 | 94,891 | 1,094 | 44,700 | Normal |
| 2020-04-03 | 395,055 | 89,067 | 999 | 44,284 | Normal |
| 2020-04-04 | 403,416 | 93,251 | 876 | 51,286 | Normal |
| 2020-04-05 | 480,172 | 109,719 | 988 | 50,911 | Slight spike |
| 2020-04-06 | 459,482 | 106,742 | 1,297 | 49,941 | Normal |
| 2020-04-07 | 445,848 | 95,640 | 1,314 | 49,326 | Normal |
| 2020-04-08 | 410,407 | 86,038 | 1,262 | 47,783 | Normal |
| 2020-04-09 | 439,057 | 88,431 | 1,234 | 48,731 | Normal |
| 2020-04-10 | 457,414 | 95,952 | 1,178 | 51,024 | Normal |

**Pattern:** Daily average ~440K events. No major gaps or unusual spikes detected in first 10 days.

---

## 9. Next Steps for dbt

**Quick Summary:**
- No deduplication needed (100% unique event_ids)
- Filter invalid store_ids (122,894 events, 0.30%)
- Flag bot sessions (840 sessions with >1000 events)
- Add dbt tests for uniqueness, temporal validity, store_id range

---

**Queries Executed:** 18 sections
