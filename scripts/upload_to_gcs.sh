#!/bin/bash
# Upload processed data files to GCS bucket

BUCKET="gs://raw_glamira"
LOCAL_DIR="data/exports"

# Upload intermediate files (creates processed/ folder automatically)
[ -f "$LOCAL_DIR/ip_list.txt" ] && gsutil cp "$LOCAL_DIR/ip_list.txt" "$BUCKET/processed/" && echo "DONE ip_list.txt"
[ -f "$LOCAL_DIR/ip_locations.csv" ] && gsutil cp "$LOCAL_DIR/ip_locations.csv" "$BUCKET/processed/" && echo "DONE ip_locations.csv"
[ -f "$LOCAL_DIR/product_url_map.csv" ] && gsutil cp "$LOCAL_DIR/product_url_map.csv" "$BUCKET/processed/" && echo "DONE product_url_map.csv"

# Upload final output (creates final/ folder automatically)
[ -f "$LOCAL_DIR/product_names.csv" ] && gsutil cp "$LOCAL_DIR/product_names.csv" "$BUCKET/final/" && echo "DONE product_names.csv"

echo "Done"
