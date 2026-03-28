"""
Product URL Extractor from MongoDB.

Extracts unique product URLs from MongoDB event tracking data for crawling.

Usage:
    python -m ingestion.product_crawler.extractor
    python -m ingestion.product_crawler.extractor --output custom_output.csv
"""

import csv
from pathlib import Path

from common.database.mongodb_client import get_mongodb_client
from common.utils.logger import get_logger
from ingestion.product_crawler import config

logger = get_logger(__name__)


def extract_product_urls(output_file: Path = None) -> int:
    """
    Extract unique product URLs from MongoDB summary collection.

    Strategy:
    - Query events with product_id or viewing_product_id
    - Fallback: referrer_url for some event types
    - First-win deduplication (MongoDB $group + $first)

    Args:
        output_file: Output CSV path (default: from config)

    Returns:
        Number of products extracted
    """
    if output_file is None:
        output_file = config.INPUT_FILE

    logger.info("Connecting to MongoDB...")
    mongo_client = get_mongodb_client()
    collection = mongo_client.get_collection("summary")

    product_urls = {}

    # ========================================================================
    # Group 1: Events with product_id + current_url
    # ========================================================================

    collection_names_group1 = [
        "view_product_detail",
        "select_product_option",
        "select_product_option_quality",
        "add_to_cart_action",
        "product_detail_recommendation_visible",
        "product_detail_recommendation_noticed"
    ]

    logger.info(f"Querying Group 1: {len(collection_names_group1)} event types...")

    # Aggregation pipeline for deduplication
    pipeline = [
        {"$match": {"collection": {"$in": collection_names_group1}}},
        {"$match": {"$or": [
            {"product_id": {"$ne": None, "$ne": ""}},
            {"viewing_product_id": {"$ne": None, "$ne": ""}}
        ]}},
        {"$project": {
            "pid": {"$ifNull": ["$product_id", "$viewing_product_id"]},
            "url": "$current_url"
        }},
        {"$match": {"url": {"$ne": None, "$ne": ""}}},
        {"$group": {
            "_id": "$pid",
            "url": {"$first": "$url"}
        }}
    ]

    cursor = collection.aggregate(pipeline, allowDiskUse=True)

    for doc in cursor:
        pid = doc["_id"]
        url = doc["url"]
        product_urls[pid] = url

    logger.info(f"Group 1: {len(product_urls)} unique products")

    # ========================================================================
    # Group 2: product_view_all_recommend_clicked with referrer_url
    # ========================================================================

    logger.info("Querying Group 2: product_view_all_recommend_clicked...")

    pipeline = [
        {"$match": {"collection": "product_view_all_recommend_clicked"}},
        {"$match": {
            "viewing_product_id": {"$ne": None, "$ne": ""},
            "referrer_url": {"$ne": None, "$ne": ""}
        }},
        {"$group": {
            "_id": "$viewing_product_id",
            "url": {"$first": "$referrer_url"}
        }}
    ]

    cursor = collection.aggregate(pipeline, allowDiskUse=True)

    added = 0
    for doc in cursor:
        pid = doc["_id"]
        url = doc["url"]

        if pid not in product_urls:
            product_urls[pid] = url
            added += 1

    logger.info(f"Group 2: {added} new products")
    logger.info(f"Total unique products: {len(product_urls)}")

    # ========================================================================
    # Export to CSV
    # ========================================================================

    output_file.parent.mkdir(parents=True, exist_ok=True)

    logger.info(f"Writing to CSV: {output_file}")
    with open(output_file, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["product_id", "url"])

        for pid, url in product_urls.items():
            writer.writerow([pid, url])

    logger.info(f"Export complete: {len(product_urls)} products → {output_file.name}")

    mongo_client.close()

    return len(product_urls)


def main():
    """Main entry point for URL extractor."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Extract product URLs from MongoDB for crawling"
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Output CSV file path (default: from config)"
    )

    args = parser.parse_args()

    output_file = Path(args.output) if args.output else None

    logger.info("Product URL Extractor")
    logger.info("=" * 70)

    count = extract_product_urls(output_file)

    logger.info("=" * 70)
    logger.info(f"Extraction complete: {count} products")


if __name__ == "__main__":
    main()
