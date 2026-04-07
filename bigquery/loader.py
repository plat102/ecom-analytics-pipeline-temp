"""
BigQuery loader CLI.

Load JSONL.gz files from GCS to BigQuery raw tables with ingested_at timestamp.

Usage:
    # Load all events files
    python bigquery/loader.py --table events

    # Load events for specific date
    python bigquery/loader.py --table events --date 20260404

    # Dry run to preview what would be loaded
    python bigquery/loader.py --table ip_locations --dry-run
"""

import argparse
import sys
from google.cloud import bigquery

from common.bigquery.loader_utils import (
    construct_gcs_uri,
    load_via_external_table,
    validate_table
)
from config.settings import GCS_BUCKET, BQ_PROJECT_ID, BQ_DATASET_ID


PROJECT_ID = BQ_PROJECT_ID
DATASET_ID = BQ_DATASET_ID


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Load JSONL.gz files from GCS to BigQuery raw tables",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument(
        "--table",
        required=True,
        choices=["events", "ip_locations", "products"],
        help="Table to load data into"
    )
    parser.add_argument(
        "--date",
        help="Date in YYYYMMDD format (optional, defaults to all files)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be done without executing"
    )

    args = parser.parse_args()

    client = bigquery.Client(project=PROJECT_ID)

    gcs_uri = construct_gcs_uri(GCS_BUCKET, args.table, args.date)

    print(f"\n{'='*60}")
    print(f"BigQuery Loader")
    print(f"{'='*60}")
    print(f"Project: {PROJECT_ID}")
    print(f"Dataset: {DATASET_ID}")
    print(f"Table: {args.table}")
    print(f"Date filter: {args.date or 'None (all files)'}")
    print(f"GCS URI: {gcs_uri}")
    print(f"Target: {PROJECT_ID}.{DATASET_ID}.{args.table}")
    print(f"{'='*60}\n")

    if args.dry_run:
        print("DRY RUN - No changes will be made")
        print(f"\nWould load from: {gcs_uri}")
        print(f"Would insert into: {PROJECT_ID}.{DATASET_ID}.{args.table}")
        return 0

    try:
        rows_inserted = load_via_external_table(
            client=client,
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
            table_name=args.table,
            gcs_uri=gcs_uri
        )

        print(f"\nLoad completed successfully!")
        print(f"Rows inserted: {rows_inserted:,}")

        print(f"\nValidating load...")
        validation = validate_table(
            client=client,
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
            table_name=args.table
        )

        print(f"\nValidation results:")
        print(f"  Total rows in table: {validation['total_rows']:,}")
        print(f"  Earliest ingestion: {validation['earliest_ingestion']}")
        print(f"  Latest ingestion: {validation['latest_ingestion']}")
        print(f"  Distinct ingestion dates: {validation['distinct_ingestion_dates']}")

        if rows_inserted == 0:
            print("\n WARNING: No rows were inserted. Check GCS URI pattern.")
            return 1

        return 0

    except Exception as e:
        print(f"\n ERROR: Load failed")
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
