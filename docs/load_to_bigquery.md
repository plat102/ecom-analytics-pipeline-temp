# Load GCS to BigQuery

Load JSONL.gz files from GCS to BigQuery with two methods: Manual CLI and Auto Cloud Function.

---

## Prerequisites

- GCS bucket: `raw_glamira` with exported data
- BigQuery dataset: `glamira_raw` (project: `ecom-analytics-tp`)
- Tables created with correct schemas
- GCP credentials configured (`gcloud auth login`)

---

## Table Schemas Overview

| Table | Schema Type | Fields | Size | Notes |
|-------|-------------|--------|------|-------|
| **events** | **Typed schema** | 57 fields (32 top-level + 25 nested) | 41.4M rows | Complex nested structures (REPEATED RECORD) |
| **ip_locations** | **Typed schema** | 5 fields | 3.2M rows | Simple flat structure |
| **products** | **Typed schema** | 36 fields | 19K rows | Flat structure with product attributes |

### Why Typed Schema for All Tables?

**Benefits:**
- **Query performance:** Columnar storage enables fast aggregations
- **Cost optimization:** Significantly cheaper than JSON_VALUE() parsing
- **Type safety:** Schema validation at insert time
- **Better analytics:** Native SQL types (INT64, FLOAT64, TIMESTAMP) vs string parsing

**Events table specifics:**
- Complex nested structures → REPEATED RECORD for `option[]` and `cart_products[]`
- Partitioned by `DATE(ingested_at)` for query performance (41M+ rows)

**IP Locations & Products:**
- Simple flat structures → Standard column definitions
- No partitioning needed (smaller tables)

---

## Method 1: Manual Load (CLI)

Use for one-time loads, backfills, or testing.

**Load All Files for a Table**

```bash
cd /path/to/ecom-analytics-pipeline

# Load events (typed schema, 41M rows)
PYTHONPATH=. poetry run python bigquery/cli/load.py --table events

# Load IP locations (typed schema, 3.2M rows)
PYTHONPATH=. poetry run python bigquery/cli/load.py --table ip_locations

# Load products (typed schema, 19K rows)
PYTHONPATH=. poetry run python bigquery/cli/load.py --table products
```

**Load Files for Specific Date**

```bash
# Load only files matching date pattern
PYTHONPATH=. poetry run python bigquery/cli/load.py --table events --date 20260404

# GCS URI pattern used:
# gs://raw_glamira/raw/events/events_20260404_part*.jsonl.gz
```

**Dry Run (Preview Without Executing)**

```bash
PYTHONPATH=. poetry run python bigquery/cli/load.py --table events --dry-run

# Output:
# DRY RUN - No changes will be made
# Would load from: gs://raw_glamira/raw/events/events_*.jsonl.gz
# Would insert into: ecom-analytics-tp.glamira_raw.events
```

---

## Method 2: Auto Load (Cloud Function)

Use for production: Auto-trigger on GCS file upload (reactive approach).

**Trigger:** GCS object finalize event on `gs://raw_glamira/raw/**`

**Routing:**
- `raw/events/events_*.jsonl.gz` → `glamira_raw.events` (typed schema, 57 fields)
- `raw/ip_locations/*.jsonl.gz` → `glamira_raw.ip_locations` (typed schema, 5 fields)
- `raw/products/*.jsonl.gz` → `glamira_raw.products` (typed schema, 36 fields)

**Duplicate Handling:** Reactive approach
- Cloud Function loads all files (including retries/re-uploads)
- Raw layer may contain duplicates
- Deduplication handled in dbt transformations later
  ```sql
  QUALIFY ROW_NUMBER() OVER (PARTITION BY _id ORDER BY ingested_at DESC) = 1
  ```

### Deploy Cloud Function

**Recommended:** Use automated deployment script

```bash
cd /path/to/ecom-analytics-pipeline/cloud_functions/gcs_to_bq

# One-command deployment (copies common/ module + deploys)
./deploy.sh
```

**Manual deployment (if needed):**

```bash
# Step 1: Copy common/ module into deployment folder
./copy_common.sh

# Step 2: Deploy function
gcloud functions deploy gcs-to-bq-loader \
  --gen2 \
  --runtime=python312 \
  --region=asia-southeast1 \
  --source=. \
  --entry-point=gcs_to_bigquery \
  --trigger-event-filters="type=google.cloud.storage.object.v1.finalized" \
  --trigger-event-filters="bucket=raw_glamira" \
  --trigger-location=asia-southeast1 \
  --memory=256MB \
  --timeout=540s
```

**Deployment time:** ~2-3 minutes

**Note:** Cloud Function uses shared libraries from `common/bigquery/`

#### Verify Deployment

```bash
# Check function status
gcloud functions list --gen2 --region=asia-southeast1 | grep gcs-to-bq

# Expected output:
# gcs-to-bq-loader  asia-southeast1  ACTIVE  ...
```

### Test Auto-Load

Upload a file to trigger the function:

```bash
# Test with IP locations file
gcloud storage cp \
  data/exports/ip_locations.csv \
  gs://raw_glamira/raw/ip_locations/ip_locations_test.jsonl.gz

# Wait 10-20 seconds for function to execute

# Check logs
gcloud functions logs read gcs-to-bq-loader \
  --gen2 \
  --region=asia-southeast1 \
  --limit=20

# Verify data loaded in BigQuery
bq query --use_legacy_sql=false \
  "SELECT COUNT(*) FROM \`ecom-analytics-tp.glamira_raw.ip_locations\`
   WHERE DATE(ingested_at) = CURRENT_DATE()"
```

### Monitor Cloud Function

**View recent logs:**
```bash
gcloud functions logs read gcs-to-bq-loader \
  --gen2 \
  --region=asia-southeast1 \
  --limit=50 \
  --format=table
```

**Cloud Logging filters:**

Successful loads:
```
resource.type="cloud_run_revision"
resource.labels.service_name="gcs-to-bq-loader"
jsonPayload.message="BigQuery load successful"
```

Failed loads:
```
resource.type="cloud_run_revision"
resource.labels.service_name="gcs-to-bq-loader"
severity="ERROR"
```

---

## GCS File Patterns

| Table | GCS Pattern | Example |
|-------|-------------|---------|
| events | `gs://raw_glamira/raw/events/events_YYYYMMDD_part*.jsonl.gz` | `events_20260404_part001.jsonl.gz` |
| ip_locations | `gs://raw_glamira/raw/ip_locations/ip_locations_*.jsonl.gz` | `ip_locations_20260405.jsonl.gz` |
| products | `gs://raw_glamira/raw/products/products_*.jsonl.gz` | `products_20260327.jsonl.gz` |

**Wildcard support:** Both manual CLI and Cloud Function support `*` wildcards in file patterns.

---

## Production Status

All tables loaded:

| Table | Rows | Status | Load Method |
|-------|------|--------|-------------|
| events | 41,432,473 | Complete | Cloud Function (auto) |
| ip_locations | 6,479,256 | Complete | Cloud Function (auto) |
| products | 18,981 | Complete | Cloud Function (auto) |

Cloud Function deployed: `gcs-to-bq-loader` (Gen2, asia-southeast1, ACTIVE)

Next step: Project 07 - dbt transformations (with deduplication) and Looker Studio viz

---

**Next:** [Verify data quality and set up dbt project - TODO](...)

**See also:**
- [export_to_gcs.md](export_to_gcs.md) - How to export data from sources to GCS
