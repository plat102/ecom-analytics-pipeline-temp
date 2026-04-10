# Export Data to GCS

Export data from MongoDB, IP processing, and web crawling to Google Cloud Storage.

---

## 1. MongoDB Events (41M docs)

```bash
# Full export
poetry run python -m ingestion.sources.mongodb_events export

# Test mode
poetry run python -m ingestion.sources.mongodb_events export --test 1000

# Resume from checkpoint
poetry run python -m ingestion.sources.mongodb_events export --resume
```

**Output:** `gs://raw_glamira/raw/events/events_20260404_part*.jsonl.gz` (415 files, ~4.5 GB)

---

## 2. IP Locations (3.2M IPs)

```bash
# Extract unique IPs
poetry run python -m ingestion.sources.ip_locations.extract_unique_ips

# Process with ip2location
poetry run python -m ingestion.sources.ip_locations.process_ip \
  --bin-file ~/data/IP-COUNTRY-REGION-CITY.BIN

# Upload to GCS
gcloud storage cp data/exports/ip_locations.csv \
  gs://raw_glamira/raw/ip_locations/ip_locations_20260405.jsonl.gz
```

**Output:** `gs://raw_glamira/raw/ip_locations/ip_locations_*.jsonl.gz` (~100 MB)

**See:** [IP Location Processing](ip_location_processing.md)

---

## 3. Products (19K products)

```bash
# Complete pipeline (extract → crawl → retry → upload)
poetry run python -m ingestion.sources.products pipeline --upload

# Or step by step
poetry run python -m ingestion.sources.products extract
poetry run python -m ingestion.sources.products crawl --concurrency 15
poetry run python -m ingestion.sources.products retry --403-only
poetry run python -m ingestion.sources.products upload --file <latest_retry_file>
```

**Output:** `gs://raw_glamira/raw/products/products_*.jsonl.gz` (~5 MB)

**See:** [Product Crawler](product_crawl.md)

---

## Running on VM

Use `tmux` for long-running exports:

```bash
tmux new -s export_session
poetry run python -m ingestion.sources.mongodb_events export
# Detach: Ctrl+B then D
# Reattach: tmux attach -t export_session
```

---

**Next:** [Load to BigQuery](load_to_bigquery.md)
