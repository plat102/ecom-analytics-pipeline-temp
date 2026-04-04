"""
Process IP addresses and enrich with geolocation data using ip2location
"""

import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import csv
import IP2Location

from common.database.mongodb_client import get_mongodb_client
from common.utils.logger import get_logger
from config import settings

logger = get_logger(__name__, 'process_ip.log')


def process_ips_with_geolocation(
    ip_file,
    bin_file,
    csv_output='ip_locations.csv',
    mongodb_collection='ip_location_data',
    batch_size=1000
):
    """
    Process IPs using ip2location library
    Output to both CSV and MongoDB

    Args:
        ip_file: Path to text file containing IP addresses (one per line)
        bin_file: Path to IP2Location BIN file
        csv_output: Output CSV filename
        mongodb_collection: MongoDB collection name for results
        batch_size: Number of documents to batch insert
    """
    logger.info("=" * 60)
    logger.info("IP Location Processing with ip2location")
    logger.info("=" * 60)

    # Initialize IP2Location
    logger.info(f"Loading IP2Location database: {bin_file}")
    ip2loc = IP2Location.IP2Location(str(bin_file))

    # Read input IPs
    logger.info(f"Reading IP addresses from: {ip_file}")
    with open(ip_file, 'r') as f:
        ip_list = [line.strip() for line in f if line.strip()]

    logger.info(f"Total IPs to process: {len(ip_list):,}")

    # Prepare output
    csv_output_path = settings.DATA_EXPORTS_DIR / csv_output
    logger.info(f"CSV output: {csv_output_path}")

    # Process IPs
    results = []
    null_country_count = 0

    logger.info("Processing IPs...")
    with open(csv_output_path, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['ip', 'country', 'region', 'city']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for idx, ip in enumerate(ip_list, 1):
            try:
                rec = ip2loc.get_all(ip)

                # Extract location data
                country = rec.country_long if rec.country_long != '-' else None
                region = rec.region if rec.region != '-' else None
                city = rec.city if rec.city != '-' else None

                if country is None:
                    null_country_count += 1

                result = {
                    'ip': ip,
                    'country': country,
                    'region': region,
                    'city': city
                }

                # Write to CSV
                writer.writerow(result)

                # Collect for MongoDB batch insert
                results.append(result)

                # Log progress every 10,000 IPs
                if idx % 10000 == 0:
                    logger.info(f"Processed {idx:,} / {len(ip_list):,} IPs")

            except Exception as e:
                logger.warning(f"Error processing IP {ip}: {e}")
                results.append({
                    'ip': ip,
                    'country': None,
                    'region': None,
                    'city': None
                })

    logger.info(f"CSV export complete: {len(results):,} records")
    logger.info(f"Null country rate: {null_country_count}/{len(results)} "
                f"({null_country_count/len(results)*100:.2f}%)")

    # Insert into MongoDB in batches
    logger.info(f"Inserting results into MongoDB collection: "
                f"{mongodb_collection}")

    mongo_client = None
    try:
        mongo_client = get_mongodb_client()
        collection = mongo_client.db[mongodb_collection]

        # Drop existing collection if exists (MVP - recreate each time)
        if mongodb_collection in mongo_client.db.list_collection_names():
            logger.info(f"Dropping existing collection: {mongodb_collection}")
            collection.drop()

        # Batch insert
        total_inserted = 0
        for i in range(0, len(results), batch_size):
            batch = results[i:i + batch_size]
            collection.insert_many(batch)
            total_inserted += len(batch)

            if (i // batch_size + 1) % 10 == 0:
                logger.info(f"Inserted {total_inserted:,} / {len(results):,} "
                            f"documents")

        logger.info(f"MongoDB insert complete: {total_inserted:,} documents")

        # Create index on ip field for fast lookups
        logger.info("Creating index on 'ip' field...")
        collection.create_index("ip", unique=True)
    finally:
        if mongo_client:
            mongo_client.close()

    logger.info("=" * 60)
    logger.info("Processing complete!")
    logger.info(f"CSV output: {csv_output_path}")
    logger.info(f"MongoDB collection: {mongodb_collection}")
    logger.info("=" * 60)


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description='Process IP addresses with geolocation'
    )
    parser.add_argument(
        '--ip-file',
        type=str,
        default='ip_list.txt',
        help='Input file with IP addresses (default: ip_list.txt)'
    )
    parser.add_argument(
        '--bin-file',
        type=str,
        required=True,
        help='Path to IP2Location BIN file'
    )
    parser.add_argument(
        '--csv-output',
        type=str,
        default='ip_locations.csv',
        help='Output CSV filename (default: ip_locations.csv)'
    )
    parser.add_argument(
        '--collection',
        type=str,
        default='ip_location_data',
        help='MongoDB collection name (default: ip_location_data)'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=1000,
        help='Batch size for MongoDB inserts (default: 1000)'
    )

    args = parser.parse_args()

    # Resolve paths
    ip_file = settings.DATA_EXPORTS_DIR / args.ip_file
    bin_file = Path(args.bin_file)

    if not ip_file.exists():
        logger.error(f"IP file not found: {ip_file}")
        logger.info("Run extract_unique_ips.py first to generate IP list")
        return

    if not bin_file.exists():
        logger.error(f"IP2Location BIN file not found: {bin_file}")
        return

    process_ips_with_geolocation(
        ip_file=ip_file,
        bin_file=bin_file,
        csv_output=args.csv_output,
        mongodb_collection=args.collection,
        batch_size=args.batch_size
    )


if __name__ == "__main__":
    main()
