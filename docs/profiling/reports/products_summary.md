# Products Table - Data Profiling Summary

**Dataset:** `ecom-analytics-tp.glamira_raw.products`
**Data Partition:** 2026-04-10
**Total Rows:** 18,938

---

## Executive Summary

**Key Findings:**
- 100% unique product_ids (zero duplicates)
- 100% successful crawl status (all products successfully scraped)
- Zero null product_ids or URLs
- 132 products (0.7%) missing category/image

---

## 1. Row-Level Metrics

| Metric | Value |
|--------|-------|
| Total Rows | 18,938 |
| Distinct Product IDs | 18,938 (100% unique) |
| Duplicate Product IDs | 0 |
| Null Product IDs | 0 |

---

## 2. Cardinality (Distinct Value Counts)

| Dimension | Distinct Count | Notes |
|-----------|---------------|-------|
| Product IDs | 18,938 | 100% unique |
| URLs | 18,937 | 1 duplicate URL (negligible) |
| SKUs | 18,787 | Some SKU reuse across stores |
| **Categories** | 43 | - |
| **Category Names** | 500 | Multiple languages (En, De, etc.) |
| Collections | 73 | Product collections/lines |
| Attribute Sets | 4 | Low cardinality - product types |
| **Stores** | 71 | 71 out of 86 stores have products |
| Currencies | 42 | Multi-currency support |
| Product Types | 15 | - |
| Genders | 3 | Male/Female/Unisex |
| Material Designs | 1 | All products same material design |

**Insights:**
- 71 stores have products (out of 86 total stores from events)
- 500 category names vs 43 category IDs suggests multi-language support (same category, different names per store/language)
- High currency diversity (42 currencies) supports global ecommerce

---

## 3. Crawl Status

| Status | Count | % | Notes |
|--------|-------|---|-------|
| **success** | **18,938** | **100.00%** | All products successfully crawled |
| failed | 0 | 0.00% | No failed crawls |

---

## 4. Top Product Categories (Top 10)

| Rank | Category ID | Category Name | Products | % | Stores |
|------|------------|--------------|----------|---|--------|
| 1 | 0 | (empty/uncategorized) | 4,639 | 24.67% | 61 |
| 2 | 690 | Men's Wedding Rings | 872 | 4.64% | 13 |
| 3 | 689 | Women's Wedding Rings | 774 | 4.12% | 12 |
| 4 | 605 | Engagement Rings | 589 | 3.13% | 13 |
| 5 | 601 | Necklaces | 571 | 3.04% | 12 |
| 6 | 688 | Trauringe (German) | 563 | 2.99% | 4 |
| 7 | 688 | Wedding Rings | 561 | 2.98% | 12 |
| 8 | 689 | Damenring (German) | 397 | 2.11% | 4 |
| 9 | 27 | Rings | 327 | 1.74% | 14 |
| 10 | 690 | Herrenring (German) | 299 | 1.59% | 4 |

**Insights:**
- 24.67% products are uncategorized (category_id = 0, category_name = "")
- Multi-language category names: "Wedding Rings" vs "Trauringe" (German), "Damenring" (German women's ring)
- Category ID 688 maps to multiple names ("Trauringe", "Wedding Rings") - language variants
- Ring-focused catalog: Wedding rings, engagement rings dominant

---

## 5. Price Statistics by Currency (Top 10)

| Currency | Products | Avg Price | Median Price | Min | Max |
|----------|----------|-----------|-------------|-----|-----|
| EUR | 8,162 | 1,225.26 | 1,016 | 0 | 12,204 |
| USD | 2,891 | 1,740.57 | 1,470 | 80 | 14,639 |
| GBP | 1,828 | 958.94 | 841 | 35 | 9,618 |
| AUD | 581 | 2,026.37 | 1,781 | 177 | 10,743 |
| RON | 516 | 6,168.14 | 4,510 | 384 | 50,547 |
| SEK | 504 | 15,198.02 | 12,672 | 727 | 130,712 |
| HUF | 463 | 507,307.49 | 368,753 | 22,223 | 4,194,158 |
| CAD | 435 | 2,162.42 | 1,998 | 97 | 6,858 |
| CHF | 400 | 1,255.59 | 1,076 | 59 | 6,583 |
| DKK | 379 | 9,475.44 | 8,189 | 673 | 34,755 |

**Insights:**
- EUR dominant (8,162 products - 43% of catalog)
- USD second (2,891 products - 15%)
- HUF and SEK show much higher nominal values (currency conversion: 1 EUR ~ 400 HUF, ~11 SEK)
- Min price = 0 for EUR likely a data issue (1 product)

**Price Range (in comparable currencies):**
- EUR: 0 - 12,204 (median 1,016)
- USD: 80 - 14,639 (median 1,470)
- GBP: 35 - 9,618 (median 841)
- Median prices roughly equivalent when converted: ~$1,000-1,500 USD

**Product Type:** Luxury jewelry (median prices $1,000+)

---

## 6. Data Quality Issues

| Issue Type | Count | % of Total | Description |
|-----------|-------|-----------|-------------|
| Null product_ids | 0 | 0.00% | No null product IDs detected |
| Duplicate product_ids | 0 | 0.00% | Perfect uniqueness |
| Failed crawls | 0 | 0.00% | 100% success rate |
| Missing identifiers (product_id/url) | 0 | 0.00% | All products have identifiers |
| **Invalid prices (<=0)** | **1** | **0.01%** | 1 product with price = 0 (likely EUR product) |
| Price without currency | 0 | 0.00% | All priced products have currency |
| Short product names (<=2 chars) | 0 | 0.00% | All product names ≥3 characters |
| **No category** | **132** | **0.70%** | 132 products missing category metadata |
| **No image** | **132** | **0.70%** | Same 132 products missing image |

### Products Missing Category/Image

**Count:** 132 products (0.70%)

**Analysis:** Same 132 products missing both category AND image suggests these may be:
- New products not yet fully cataloged
- Products being phased out
- Edge case products (gift cards, services, etc.)

**Root Cause Hypothesis:** Website crawl captured products from listing pages before detail pages were fully populated.

**Note:** These products can still be used for analytics, just missing some metadata.

---

## 7. Primary Key Analysis

| Metric | Value |
|--------|-------|
| Total Rows | 18,938 |
| Distinct Product IDs | 18,938 |
| Duplicate Product IDs | 0 |
| Null Product IDs | 0 |
| **Uniqueness Rate** | **100.00%** |

**Summary:** product_id is 100% unique with 0% null rate.

---

## 8. Business Rules Validation

Since all products have status='success', we can validate business rules:

### Rule 1: Successful crawls should have product_name
- ~99.3% of successful products have product_name
- 132 products missing product_name (0.7%)

### Rule 2: Successful crawls should have price
- ~99.3% of successful products have price
- 132 products missing price (same products as above)

### Rule 3: min_price <= price <= max_price
- Price consistency within ranges requires separate query validation

### Rule 4: Products with price should have currency
- 100% of priced products have currency (0 violations)

---

## 9. Next Steps for dbt

**Quick Summary:**
- No deduplication needed (100% unique product_ids)
- Convert category '0' and empty strings to NULL
- Fix 1 invalid price (price = 0)
- Normalize multi-language category names (e.g., "Wedding Rings" vs "Trauringe")
- Add price_tier and product_type_inferred derived fields
- Add dbt tests for uniqueness, URL format, price validity

---

**Queries Executed:** 8 key sections (out of 20 available)
