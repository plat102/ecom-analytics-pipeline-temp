from common.database.mongodb_client import get_mongodb_client
from common.utils.logger import get_logger

logger = get_logger(__name__)


def main():
    logger.info("Getting total document count...")

    mongo = get_mongodb_client()
    collection = mongo.get_collection()

    try:
        # Fast estimate
        total_count = collection.estimated_document_count()
        logger.info(f"Estimated count: {total_count:,}")

        # Exact count (slower)
        exact_count = collection.count_documents({})
        logger.info(f"Exact count: {exact_count:,}")

    finally:
        mongo.close()


if __name__ == "__main__":
    main()
