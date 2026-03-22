"""
Extract unique product URLs from MongoDB summary collection.

Output: CSV file with product_id and URL mapping
"""

import csv
import sys
from pathlib import Path

# Add project root to sys.path for imports
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from common.database.mongodb_client import get_mongodb_client
from common.utils.logger import get_logger
from config import settings

logger = get_logger(__name__)

# Output path
OUTPUT_DIR = settings.DATA_EXPORTS_DIR
OUTPUT_FILE = OUTPUT_DIR / "product_url_map.csv"


def extract_product_urls():
    """
    Extract unique product URLs from MongoDB.

    Strategy:
    - Query summary collection with collection field filter
    - Fallback: product_id or viewing_product_id per document
    - First-win deduplication
    """

    logger.info("Connecting to MongoDB...")
    mongo_client = get_mongodb_client()
    collection = mongo_client.get_collection("summary")

    product_urls = {}

    # Group 1: 6 collections with product_id + current_url
    collection_names_group1 = [
        "view_product_detail",
        "select_product_option",
        "select_product_option_quality",
        "add_to_cart_action",
        "product_detail_recommendation_visible",
        "product_detail_recommendation_noticed"
    ]

    logger.info(f"Querying collections group 1: {len(collection_names_group1)} event types")

    # Use aggregation for deduplication (MongoDB handles it)
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

    logger.info(f"Group 1 complete: {len(product_urls)} unique products")

    # Group 2: product_view_all_recommend_clicked with viewing_product_id + referrer_url
    logger.info("Querying collection: product_view_all_recommend_clicked")

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

    logger.info(f"Group 2 complete: {added} new products")
    logger.info(f"Total unique products: {len(product_urls)}")

    # Export to CSV
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    logger.info(f"Writing to CSV: {OUTPUT_FILE}")
    with open(OUTPUT_FILE, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['product_id', 'url'])

        for pid, url in product_urls.items():
            writer.writerow([pid, url])

    logger.info(f"Export complete: {len(product_urls)} products written to {OUTPUT_FILE.name}")

    mongo_client.close()


if __name__ == "__main__":
    extract_product_urls()
