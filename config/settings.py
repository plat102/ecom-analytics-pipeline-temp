"""
Application Settings
Load environment variables and define project-wide settings
"""

from dotenv import load_dotenv
import os
from pathlib import Path

# Load environment variables from .env file
PROJECT_ROOT = Path(__file__).parent.parent
ENV_PATH = PROJECT_ROOT / '.env'

if ENV_PATH.exists():
    load_dotenv(ENV_PATH)
else:
    print(f"Warning: .env file not found at {ENV_PATH}")

# MongoDB Configuration
MONGO_HOST = os.getenv('MONGO_HOST')
MONGO_PORT = int(os.getenv('MONGO_PORT', '27017'))
MONGO_USERNAME = os.getenv('MONGO_USERNAME')
MONGO_PASSWORD = os.getenv('MONGO_PASSWORD')
MONGO_AUTH_DB = os.getenv('MONGO_AUTH_DB', 'admin')
MONGO_DATABASE = os.getenv('MONGO_DATABASE')
MONGO_COLLECTION = os.getenv('MONGO_COLLECTION', 'summary')

# Directory Paths
DOCS_DIR = PROJECT_ROOT / os.getenv('DOCS_DIR', 'docs')
LOGS_DIR = PROJECT_ROOT / os.getenv('LOGS_DIR', 'logs')
DATA_DIR = PROJECT_ROOT / os.getenv('DATA_DIR', 'data')
DATA_RAW_DIR = DATA_DIR / 'raw'
DATA_PROCESSED_DIR = DATA_DIR / 'processed'
DATA_EXPORTS_DIR = DATA_DIR / 'exports'

# Google Cloud Storage Configuration
GCS_BUCKET = os.getenv('GCS_BUCKET', 'raw_glamira')

# Logging Configuration
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

# Create directories if they don't exist
for directory in [DOCS_DIR, LOGS_DIR, DATA_DIR, DATA_RAW_DIR, DATA_PROCESSED_DIR, DATA_EXPORTS_DIR]:
    directory.mkdir(parents=True, exist_ok=True)
