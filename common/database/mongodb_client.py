"""
Provides a reusable MongoDB connection for the entire application
"""

from pymongo import MongoClient
from urllib.parse import quote_plus
from config import settings
import logging

logger = logging.getLogger(__name__)


class MongoDBClient:
    """MongoDB client"""

    _instance = None
    _client = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if self._client is None:
            self._connect()

    def _connect(self):
        """Establish MongoDB connection"""
        try:
            username = quote_plus(settings.MONGO_USERNAME)
            password = quote_plus(settings.MONGO_PASSWORD)

            uri = (f"mongodb://{username}:{password}@"
                   f"{settings.MONGO_HOST}:{settings.MONGO_PORT}/"
                   f"?authSource={settings.MONGO_AUTH_DB}")

            self._client = MongoClient(uri, serverSelectionTimeoutMS=5000)

            # Test connection
            self._client.server_info()

            logger.info(f"Connected to MongoDB at {settings.MONGO_HOST}")

        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise

    @property
    def client(self):
        """Get MongoDB client"""
        if self._client is None:
            self._connect()
        return self._client

    @property
    def db(self):
        """Get default database"""
        return self.client[settings.MONGO_DATABASE]

    def get_collection(self, collection_name=None):
        """
        Get a collection from the database
        """
        if collection_name is None:
            collection_name = settings.MONGO_COLLECTION
        return self.db[collection_name]

    def close(self):
        """Close MongoDB connection"""
        if self._client:
            self._client.close()
            self._client = None
            logger.info("MongoDB connection closed")


# Convenience function
def get_mongodb_client():
    """Get MongoDB client instance"""
    return MongoDBClient()
