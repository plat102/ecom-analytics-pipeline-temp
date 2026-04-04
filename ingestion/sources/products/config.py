"""
Product Crawler Configuration.

Centralized configuration for all crawler-related constants and settings.
Supports environment variable overrides for production deployment.
"""

import os

# Import project-wide settings
from config import settings

# ============================================================================
# PATHS
# ============================================================================

# Output directory for all crawler data
OUTPUT_DIR = settings.DATA_EXPORTS_DIR

# Default input/output files
INPUT_FILE = OUTPUT_DIR / "product_url_map.csv"
CHECKPOINT_FILE = OUTPUT_DIR / "crawl_checkpoint.json"
FULL_CRAWL_OUTPUT = OUTPUT_DIR / "full_crawl_results.json"

# Note: Retry outputs use timestamp-based naming (e.g., retry_20260329_143022.json)
# to avoid overwriting and support unlimited retry iterations

# ============================================================================
# CONCURRENCY SETTINGS
# ============================================================================

# Concurrency limits (max concurrent HTTP requests)
CONCURRENCY_TEST = int(os.getenv("CRAWLER_CONCURRENCY_TEST", 20))  # Test mode: higher throughput
CONCURRENCY_FULL = int(os.getenv("CRAWLER_CONCURRENCY_FULL", 15))  # Full crawl: safer rate
CONCURRENCY_DLQ = int(os.getenv("CRAWLER_CONCURRENCY_DLQ", 5))     # DLQ retry: lowest rate

# ============================================================================
# RATE LIMITING
# ============================================================================

# Random delay between requests (seconds)
DELAY_MIN = float(os.getenv("CRAWLER_DELAY_MIN", 0.5))
DELAY_MAX = float(os.getenv("CRAWLER_DELAY_MAX", 1.5))

# DLQ-specific delays (longer for hard cases)
DLQ_DELAY_MIN = float(os.getenv("CRAWLER_DLQ_DELAY_MIN", 1.0))
DLQ_DELAY_MAX = float(os.getenv("CRAWLER_DLQ_DELAY_MAX", 3.0))

# ============================================================================
# RETRY SETTINGS
# ============================================================================

# Retry configuration for 429/503/403 errors
MAX_RETRIES = int(os.getenv("CRAWLER_MAX_RETRIES", 3))
BACKOFF_BASE = float(os.getenv("CRAWLER_BACKOFF_BASE", 2.0))  # Exponential backoff base

# ============================================================================
# CHECKPOINT SETTINGS
# ============================================================================

# Save checkpoint every N products
CHECKPOINT_INTERVAL = int(os.getenv("CRAWLER_CHECKPOINT_INTERVAL", 100))

# ============================================================================
# USER AGENTS POOL
# ============================================================================

# User-Agent pool (12 browsers for rotation)
USER_AGENTS = [
    # Chrome on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    # Chrome on Mac
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    # Firefox on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
    # Firefox on Mac
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:120.0) Gecko/20100101 Firefox/120.0",
    # Safari on Mac
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    # Edge on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0",
]

# ============================================================================
# URL CLEANING
# ============================================================================

# Tracking parameters to remove (reduce WAF detection)
TRACKING_PARAMS = [
    'fbclid', 'utm_source', 'utm_medium', 'utm_campaign', 'utm_content', 'utm_term',
    'itm_source', 'itm_medium', 'itm_campaign', 'itm_content',
    'gclid', 'gclsrc', '_ga', 'mc_cid', 'mc_eid',
    'msclkid', 'zanpid', 'aff_id', 'ref', 'source'
]

# ============================================================================
# GOOGLE CLOUD STORAGE SETTINGS
# ============================================================================

# GCS bucket name (should be configured via environment variable on server)
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "ecom-analytics-data-lake")

# Destination prefix on GCS (Data Lake path structure)
# Format: raw/glamira/products/ -> organized by source/domain/data_type
GCS_DESTINATION_PREFIX = os.getenv("GCS_DESTINATION_PREFIX", "raw/glamira/products")

# Auto-upload to GCS after crawling (default: False, enable via env var)
AUTO_UPLOAD_TO_GCS = os.getenv("AUTO_UPLOAD_TO_GCS", "false").lower() in ("true", "1", "yes")
