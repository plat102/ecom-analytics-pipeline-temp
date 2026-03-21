# Data Dictionary

**Database:** `countly`
**Collection:** `summary`
**Last Updated:** 2026-03-21

---

## Full Collection Overview

E-commerce analytics events from Glamira online stores.

- **Total Documents:** 41,432,473
- **Data Coverage:** March 31, 2020 - June 4, 2020 (65 days)
- **Event Types:** 27 distinct collection types
- **Stores:** 86 unique store IDs

---

*Metrics calculated on entire collection (41M+ documents)*

### Total Document Count

```
Estimated Count: 41,432,473
Query Time: ~1 second
```

**Method:** `db.summary.estimated_document_count()`
- **Script:** [01_total_count.txt](../scripts/explore_raw_glamira/01_total_count.txt)

---

### Distinct Event Types

**Total:** 27 event types

| Event Type                              |      Count |      % | Lifecycle      | Description                              |
|-----------------------------------------|-----------:|-------:|----------------|------------------------------------------|
| `view_listing_page`                     | 11,259,694 | 27.18% | Browsing       | Visit listing page                       |
| `view_product_detail`                   | 10,944,427 | 26.42% | Product        | View product detail page                 |
| `select_product_option`                 |  8,844,342 | 21.35% | Product        | User selects product option              |
| `select_product_option_quality`         |  2,231,825 |  5.39% | Product        | User selects product quality option      |
| `view_static_page`                      |  1,451,565 |  3.50% | Browsing       | Visit static page                        |
| `view_landing_page`                     |  1,434,230 |  3.46% | Browsing       | Visit landing page                       |
| `product_detail_recommendation_visible` |  1,302,362 |  3.14% | Recommendation | Recommendation visible on product detail |
| `view_home_page`                        |  1,053,420 |  2.54% | Browsing       | Visit home page                          |
| `listing_page_recommendation_visible`   |    718,048 |  1.73% | Recommendation | Recommendation visible on listing page   |
| `product_detail_recommendation_noticed` |    490,780 |  1.18% | Recommendation | Recommendation noticed on product detail |
| `view_shopping_cart`                    |    343,077 |  0.83% | Product        | View shopping cart                       |
| `landing_page_recommendation_visible`   |    314,999 |  0.76% | Recommendation | Recommendation visible on landing page   |
| `search_box_action`                     |    238,308 |  0.58% | Search         | User performs search                     |
| `add_to_cart_action`                    |    187,901 |  0.45% | Product        | User adds product to cart                |
| `product_detail_recommendation_clicked` |    179,228 |  0.43% | Product        | Click recommendation on product detail   |
| `view_my_account`                       |    112,066 |  0.27% | Browsing       | Visit account page                       |
| `checkout`                              |     88,540 |  0.21% | Transaction    | User proceeds to checkout                |
| `landing_page_recommendation_noticed`   |     58,186 |  0.14% | Recommendation | Recommendation noticed on landing page   |
| `listing_page_recommendation_noticed`   |     39,819 |  0.10% | Recommendation | Recommendation noticed on listing page   |
| `view_all_recommend`                    |     33,664 |  0.08% | Recommendation | View all recommendations                 |
| `checkout_success`                      |     26,079 |  0.06% | Transaction    | Order successfully completed             |
| `listing_page_recommendation_clicked`   |     25,545 |  0.06% | Product        | Click recommendation on listing page     |
| `landing_page_recommendation_clicked`   |     20,128 |  0.05% | Product        | Click recommendation on landing page     |
| `product_view_all_recommend_clicked`    |     16,682 |  0.04% | Product        | Click "view all" recommendations         |
| `view_sorting_relevance`                |     15,284 |  0.04% | Browsing       | Sort by relevance                        |
| `sorting_relevance_click_action`        |      1,713 |  0.00% | Search         | User clicks on sorting relevance         |
| `back_to_product_action`                |        561 |  0.00% | Search         | User navigates back to product           |


- **Top 3 events** account for 75% of all events:
  - `view_listing_page` (27%),
  - `view_product_detail` (26%),
  - `select_product_option` (21%)
- **Conversion events** are rare: `checkout` (0.21%), `checkout_success` (0.06%)
- **Recommendation events** show engagement: visible (5.63%), noticed (1.42%), clicked (0.52%)


**Method:** `db.summary.aggregate([{$group: {_id: "$collection", count: {$sum: 1}}}, {$sort: {count: -1}}])`
- **Script:** [03_distinct_collections.txt](../scripts/explore_raw_glamira/03_distinct_collections.txt)

#### Event Lifecycle Summary

| Stage          | Events     | % of Total | Description                                 |
|----------------|------------|------------|---------------------------------------------|
| Browsing       | 14,327,162 | 34.58%     | Navigation & page views                     |
| Product        | 22,450,822 | 54.18%     | Product interaction, configuration & cart   |
| Recommendation | 3,554,008  | 8.58%      | Recommendation visibility & user engagement |
| Search         | 240,582    | 0.58%      | Search & navigation actions                 |
| Transaction    | 114,619    | 0.28%      | Checkout & purchase completion              |

**Journey Insights:**
- **Product-focused**: 54% product interaction, 35% browsing, 9% recommendation engagement
- **Recommendation system**: 8.6% of events are recommendation-related (visible, noticed, clicked)
- **Low search dependency**: Only 0.58% search actions - users prefer browsing
- **Conversion**: 0.28% checkout events, 0.06% successful purchases

---

### Timestamp Range

```
Min Timestamp: 1585699201 (2020-03-31 07:58:21 UTC)
Max Timestamp: 1591266092 (2020-06-04 12:21:32 UTC)
Data Coverage: 65 days
```

**Key Details:**
- **Earliest event:** `select_product_option` on March 31, 2020
- **Latest event:** `view_product_detail` on June 4, 2020
- **Format:** Unix timestamp (seconds since epoch)

**Method:** Sort by `time_stamp` field (ascending/descending) and get first document
- **Script:** [06_timestamp_range.js](../scripts/explore_raw_glamira/06_timestamp_range.js)

---

### Product

**Total Distinct Products:** 19,418

**Method:** Count distinct `product_id` values
- **Script:** [09_distinct_product_count.js](../scripts/explore_raw_glamira/09_distinct_product_count.js)

---

### Store Information

**Total Stores:** 86 distinct store IDs

**What is `store_id`?**
Country/region localized websites (Top-Level domains)
- Each store ID maps to a different Glamira domain (glamira.de, glamira.fr, glamira.com,...)
- Glamira operates 86 localized e-commerce sites across different countries/regions

**Top 10 Stores by Event Volume:**

| Store ID | Event Count | % of Total | Domain         | Country/Region     |
|----------|-------------|------------|----------------|--------------------|
| 6        | 5,162,382   | 12.46%     | glamira.de     | Germany            |
| 7        | 3,359,850   | 8.11%      | glamira.co.uk  | United Kingdom     |
| 12       | 2,509,355   | 6.06%      | glamira.fr     | France             |
| 41       | 2,360,349   | 5.70%      | glamira.com    | International (US) |
| 14       | 1,866,741   | 4.51%      | glamira.it     | Italy              |
| 8        | 1,680,873   | 4.06%      | glamira.es     | Spain              |
| 19       | 1,537,060   | 3.71%      | glamira.se     | Sweden             |
| 29       | 1,412,478   | 3.41%      | glamira.com.au | Australia          |
| 51       | 1,378,809   | 3.33%      | glamira.ro     | Romania            |
| 11       | 1,246,968   | 3.01%      | glamira.nl     | Netherlands        |

**Distribution Insights:**
- **Top 3 markets:** Germany (12.46%), UK (8.11%), France (6.06%)
- **Top 10 stores** account for 54.36% of all events
- **Long tail:** 76 stores have <1M events each
- **Store range:** ID 1 to 101 (non-continuous)
- **Smallest stores:** ID 78 and 24 with only 1 event each

**Complete Store List:** 1, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16, 17, 19, 24, 25, 26, 27, 29, 30, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101

**Method:** `db.summary.aggregate([{$group: {_id: "$store_id", count: {$sum: 1}}}, {$sort: {count: -1}}])`
- **Script:** [07_distinct_stores.js](../scripts/explore_raw_glamira/07_distinct_stores.js)

---

## Sample-based Analysis

*Metrics based on random sampling for performance*

**Why Sampling?**

With 41M+ documents, analyzing the full collection would be slow. We use statistical sampling:

| Analysis          | Sample Size                   |
|-------------------|-------------------------------|
| Field List        | 10 per event type (270 total) |
| Null Rates        | 50,000 (0.12%)                |
| Nested Structures | 10 docs per structure         |



---

### Fields List

**Total Fields:** 32
- User data: 9 fields
- Interaction data: 23 fields
  
**Sampling Method:** Stratified sampling (10 docs per event type)

#### User Data

Information about the user, session, and context.

| Field Name      | Data Type(s)     | Category       | Notes                                        |
|-----------------|------------------|----------------|----------------------------------------------|
| `device_id`     | string           | Identity       | User device UUID                             |
| `user_id_db`    | string           | Identity       | User ID in database                          |
| `email_address` | string           | Identity       | User email (when available)                  |
| `ip`            | string           | Identity       | User IP address                              |
| `store_id`      | string           | Context        | Store/locale ID (86 distinct values)         |
| `user_agent`    | string           | Context        | Browser user agent                           |
| `resolution`    | string           | Context        | Screen resolution (e.g., 1920x1080)          |
| `utm_source`    | boolean, string  | Context        | UTM source for marketing attribution         |
| `utm_medium`    | boolean, string  | Context        | UTM medium for marketing attribution         |

#### Interaction Data

Events, products, cart, recommendations, and system metadata.

| Field Name                        | Data Type(s)          | Category       | Notes                                          |
|-----------------------------------|-----------------------|----------------|------------------------------------------------|
| `_id`                             | object (ObjectId)     | System         | MongoDB document ID                            |
| `api_version`                     | string                | System         | "1.0"                                          |
| `collect_id`                      | string                | System         | Collection ID                                  |
| `collection`                      | string                | System         | Event type (27 distinct values)                |
| `time_stamp`                      | number                | System         | Unix timestamp (seconds)                       |
| `local_time`                      | string                | System         | Local timestamp (e.g., "2020-06-04 12:21:27")  |
| `current_url`                     | string                | Navigation     | Current page URL                               |
| `referrer_url`                    | string                | Navigation     | Referrer URL (source page)                     |
| `product_id`                      | string                | Product        | Product ID                                     |
| `viewing_product_id`              | string                | Product        | Currently viewed product ID                    |
| `option`                          | object (array/object) | Product        | Product options selected                       |
| `price`                           | string                | Product        | Product price (locale-formatted)               |
| `currency`                        | string                | Product        | Currency symbol (€, $, £, kr)                  |
| `cat_id`                          | object (null)         | Product        | Category ID (mostly null)                      |
| `key_search`                      | object (null/string)  | Product        | Search keyword                                 |
| `cart_products`                   | object (array)        | Cart/Checkout  | Array of products in cart                      |
| `order_id`                        | string                | Cart/Checkout  | Order ID for checkout events                   |
| `is_paypal`                       | object (null/boolean) | Cart/Checkout  | PayPal payment flag                            |
| `recommendation`                  | boolean               | Recommendation | Recommendation flag                            |
| `show_recommendation`             | string                | Recommendation | Recommendation visibility flag                 |
| `recommendation_product_id`       | string                | Recommendation | Recommended product ID                         |
| `recommendation_product_position` | number                | Recommendation | Position in recommendation list                |
| `recommendation_clicked_position` | object (number/null)  | Recommendation | Position of clicked recommendation             |

**Method:** Stratified sampling (10 docs per event type to ensure all fields captured)
- **Script:** [04_field_list.js](../scripts/explore_raw_glamira/04_field_list.js)

---

### Null/Missing Rates for Key Fields

**Sample Size:** 50,000 documents (0.12% of 41M total)

| Field                | Null Count | Null Rate (%) | Notes                       |
|----------------------|------------|---------------|-----------------------------|
| `ip`                 | 0          | 0.00%         | Always present              |
| `collection`         | 0          | 0.00%         | Always present (event type) |
| `current_url`        | 0          | 0.00%         | Always present              |
| `store_id`           | 0          | 0.00%         | Always present              |
| `time_stamp`         | 0          | 0.00%         | Always present              |
| `referrer_url`       | 4,599      | 9.20%         | Missing for direct traffic  |
| `product_id`         | 23,051     | 46.10%        | Only for product events     |
| `viewing_product_id` | 47,565     | 95.13%        | Rare field                  |

**Key Observations:**
- **Mandatory fields** (0% null): `ip`, `collection`, `current_url`, `store_id`, `time_stamp`
- **Contextual field**: `product_id` (~46% null)
  - may only present for product-related events like `view_product_detail`, `add_to_cart_action`
- **Rarely used**: `viewing_product_id` (~95% null) - special tracking field
- **Direct traffic**: `referrer_url` (~9% null) - missing when users arrive directly

**Method:** Random sampling with null rate calculation
- **Script:** [05_null_rates.py](../scripts/explore_raw_glamira/05_null_rates.py)

---

### Nested Field Structures

- **Method:** Sample-based structure analysis
- **Script:** [08_nested_structures.js](../scripts/explore_raw_glamira/08_nested_structures.js)


#### Field: `option`

**Type:** Array or Object (varies by context)

**Array Structure** - Used for product configuration events:
```json
[
  {
    "option_label": "alloy",
    "option_id": "187492",
    "value_label": "red-750",
    "value_id": "1593626"
  },
  {
    "option_label": "stone/diamonds",
    "option_id": "57695",
    "value_label": "diamond-Brillant",
    "value_id": "308213",
    "quality": "A",              // Optional - for gemstones
    "quality_label": "I"         // Optional - quality grade
  }
]
```

**Object Structure** - Used for filtering/browsing:
```json
{
  "alloy": "",
  "diamond": "diamond-Brillant",
  "shapediamond": ""
}
```

**Key Differences:**
- **Array format:** Detailed product configuration with IDs - used in `select_product_option`, `add_to_cart_action`, `checkout`
- **Object format:** Simple key-value filters - used in `view_listing_page` for category browsing
- **Quality fields:** Only present for some options like diamond (quality grades like AAAA, VVS, I, etc.)
- **Empty values:** Can be empty string `""` when option not selected

**Common Options:**
- `alloy`: Metal type (e.g., "white-585", "red-750", "yellow-375")
- `diamond`: Gemstone type (e.g., "diamond-Brillant", "ruby", "sapphire")
- `shapediamond`: Diamond shape (e.g., "round", "princess")
- `stone/diamonds`: Combined gemstone option

---

#### Field: `cart_products`

**Type:** Array of objects

**Present in:** `checkout`, `checkout_success`, `view_shopping_cart`

**Structure:**
```json
[
  {
    "product_id": 103324,
    "amount": 1,              // Optional (quantity)
    "price": "880.00",        // Optional (locale-formatted)
    "currency": "£",          // Optional (symbol)
    "option": [
      {
        "option_label": "diamond",
        "option_id": 261151,
        "value_label": "Swarovsky Cristall",
        "value_id": 2166253
      },
      {
        "option_label": "alloy",
        "option_id": 261154,
        "value_label": "Weißgold 585",
        "value_id": 2166328
      }
    ]
  }
]
```

**Key Characteristics:**
- **Required field:** Only `product_id` is always present
- **Optional fields:** `amount`, `price`, `currency` may be missing
- **Option structure:** Same array format as product configuration events
- **Multiple items:** Can contain multiple products (including duplicates)
- **Empty cart:** Can be empty array `[]` when no items in cart

**Examples:**

*Single item cart:*
```json
[{"product_id": 91041, "option": [...]}]
```

*Multi-item cart:*
```json
[
  {"product_id": 97471, "option": [...]},
  {"product_id": 98502, "option": [...]},
  {"product_id": 98497, "option": [...]}
]
```

*Cart with duplicates (same product added multiple times):*
```json
[
  {"product_id": 90475, "option": [...]},
  {"product_id": 90475, "option": [...]},
  {"product_id": 90476, "option": [...]}
]
```

**Fields:**
- `product_id`: int (product identifier)
- `amount`: int (quantity, typically 1 if present)
- `price`: string (decimal with locale formatting)
- `currency`: string (symbol: €, £, $, etc.)
- `option`: array of option objects (same structure as Field: `option`)


---

### Sample Documents by Event Type

To get sample documents for each event type:

```bash
# Python (with logging)
python scripts/explore_raw_glamira/02_sample_documents.py

# MongoDB shell
db.summary.findOne({ collection: "add_to_cart_action" })
db.summary.findOne({ collection: "checkout_success" })
```

**Script:** [08_nested_structures.js](../scripts/explore_raw_glamira/08_nested_structures.js)

---

## Notes

### Data Quality
- **Timestamps:** Unix epoch format (seconds)
- **URL encoding:** Special characters properly encoded
- **Currency symbols:** UTF-8 strings (€, £, $, kr)
- **Empty values:** Both `[]` and `null` used

### Performance Tips
- Full collection queries (41M docs) can be slow
- Use sampling for exploration
- Use `estimated_document_count()` for total count
- Indexes exist on: `time_stamp`, `collection`, `store_id`

### How to Reproduce

All queries can be reproduced using:
```
scripts/explore_raw_glamira/
  ├── *.py  - Python version (with logging)
  └── *.js  - MongoDB shell version
```
