"""
Extract Unique IP Addresses from MongoDB
Export to text file for processing
"""

import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from common.database.mongodb_client import get_mongodb_client
from common.utils.logger import get_logger
from config import settings

logger = get_logger(__name__, 'extract_unique_ips.log')


def extract_unique_ips(output_file='ip_list.txt', limit=None):
    """
    Extract unique IP addresses from MongoDB collection

    Args:
        output_file: Output file path (default: ip_list.txt)
        limit: Optional limit for testing (None = all IPs)
    """
    logger.info("=" * 60)
    logger.info("Extracting Unique IP Addresses from MongoDB")
    logger.info("=" * 60)

    mongo_client = None
    try:
        # Connect to MongoDB
        mongo_client = get_mongodb_client()
        collection = mongo_client.get_collection()

        logger.info(f"Connected to database: {settings.MONGO_DATABASE}")
        logger.info(f"Collection: {settings.MONGO_COLLECTION}")

        # Query distinct IPs using aggregation (to avoid 16MB limit)
        logger.info("Querying distinct IP addresses using aggregation...")
        logger.info("This may take several minutes for 41M+ documents...")

        pipeline = [
            {"$group": {"_id": "$ip"}},
            {"$sort": {"_id": 1}}
        ]

        if limit:
            pipeline.append({"$limit": limit})

        cursor = collection.aggregate(pipeline, allowDiskUse=True)
        unique_ips = [doc["_id"] for doc in cursor]

        logger.info(f"Found {len(unique_ips):,} unique IP addresses")

        # Write to file
        output_path = settings.DATA_EXPORTS_DIR / output_file
        logger.info(f"Writing IPs to {output_path}")

        with open(output_path, 'w') as f:
            for ip in unique_ips:
                f.write(f"{ip}\n")

        logger.info(f"Successfully exported {len(unique_ips):,} IPs")
        logger.info(f"Output file: {output_path}")

        return output_path

    except Exception as e:
        logger.error(f"Error extracting IPs: {e}")
        raise
    finally:
        if mongo_client:
            mongo_client.close()


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description='Extract unique IP addresses from MongoDB'
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='Limit number of IPs for testing (default: all IPs)'
    )
    parser.add_argument(
        '--output',
        type=str,
        default='ip_list.txt',
        help='Output file name (default: ip_list.txt)'
    )

    args = parser.parse_args()

    extract_unique_ips(
        output_file=args.output,
        limit=args.limit
    )


if __name__ == "__main__":
    main()
