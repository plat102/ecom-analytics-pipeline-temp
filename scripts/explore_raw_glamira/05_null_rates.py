"""
Query: Check null/missing rates for key fields
"""

from common.database.mongodb_client import get_mongodb_client
from common.utils.logger import get_logger

logger = get_logger(__name__)


def main():
    logger.info("Checking null rates for key fields...")

    mongo = get_mongodb_client()
    collection = mongo.get_collection()

    try:
        # Key fields to check
        key_fields = [
            "ip",
            "collection",
            "product_id",
            "viewing_product_id",
            "current_url",
            "referrer_url",
            "store_id",
            "time_stamp"
        ]

        # Get sample for analysis (larger sample for better accuracy)
        sample_size = 50000
        pipeline = [{"$sample": {"size": sample_size}}]
        samples = list(collection.aggregate(pipeline))

        logger.info(f"Analyzing {len(samples)} sample documents\n")
        logger.info("-" * 80)
        logger.info(f"{'Field':<25} {'Null Count':<15} {'Null Rate':<15}")
        logger.info("-" * 80)

        for field in key_fields:
            null_count = 0
            for doc in samples:
                if field not in doc or doc[field] is None or doc[field] == "":
                    null_count += 1

            null_rate = (null_count / len(samples)) * 100
            logger.info(f"{field:<25} {null_count:<15} {null_rate:>6.2f}%")

    finally:
        mongo.close()


if __name__ == "__main__":
    main()
