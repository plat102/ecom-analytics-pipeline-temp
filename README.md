# Glamira E-commerce Analytics Pipeline

End-to-end data pipeline for analyzing user behavior from the Glamira dataset.

---

## Infrastructure

| Component    | Service              | Details                               |
|--------------|----------------------|---------------------------------------|
| Data storage | Google Cloud Storage | Bucket `raw_glamira`, asia-southeast1 |
| Database     | MongoDB 7.0          | GCP VM `e2-standard-2`, us-central1-a |

### Project Structure

```
ecom-analytics-pipeline/
├── common/                    # Shared utilities
├── ingestion/
│   ├── ip_location/           # IP geolocation enrichment
│   └── product_crawler/       # Crawl product names from Glamira web
├── scripts/                   # One-off scripts
└── docs/                      # Data dictionary, setup guides
```

---

## Getting Started

### Prerequisites

- GCP account with billing enabled
- Python 3.12+
- [Poetry](https://python-poetry.org/)
- [Google Cloud CLI](https://cloud.google.com/sdk/docs/install)

### Infrastructure Setup

See [`docs/setup_guide.md`](docs/setup_guide.md) for full step-by-step process:
- GCS bucket creation and data upload
- GCP VM provisioning and firewall configuration
- MongoDB installation and authentication setup
- Raw data import

### Local & VM

```bash
git clone https://github.com/plat102/ecom-analytics-pipeline.git
cd ecom-analytics-pipeline
poetry install
cp .env.example .env
# Fill in MONGO_URI and other variables
```

---

## Data Collection & Storage

### Dataset

|             |                                                                            |
|-------------|----------------------------------------------------------------------------|
| Source      | `glamira_ubl_oct2019_nov2019.tar.gz` (5.1 GB compressed, ~32 GB extracted) |
| Database    | `countly` / Collection `summary`                                           |
| Documents   | 41,432,473                                                                 |
| Period      | March 31 – June 4, 2020 (65 days)                                          |
| Event types | 27                                                                         |
| Stores      | 86 country-specific domains                                                |

#### Data Dictionary 
View: [data_dictionary.md](docs/data_dictionary.md)
for schema, field types, event types, store mapping

### Ingestion Scripts

**IP Location** - enrich 3.2M unique IPs with country/region/city:

See: [ip_location_processing.md](docs/ip_location_processing.md)

```bash
poetry run python ingestion/ip_location/extract_unique_ips.py
poetry run python ingestion/ip_location/process_ip.py \
  --bin-file /path/to/IP-COUNTRY-REGION-CITY.BIN
```

Output:
- File: `data/exports/ip_locations.csv`
- MongoDB collection: `ip_location_data`


---

**Product Crawler** - crawl product names for ~19K products from Glamira website:

See: [product_crawl.md](docs/product_crawl.md)

```bash
poetry run python ingestion/product_crawler/extract_product_urls.py
poetry run python ingestion/product_crawler/crawl_products_parallel.py --workers 5
```

Output: 
- Product names only: `data/exports/product_names.csv`
- Product data
  - Sample: 

---

## Data Pipeline & Warehouse

...

## Data Transformation & Visualization
...

