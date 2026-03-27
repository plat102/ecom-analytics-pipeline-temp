"""
Shared utilities for product crawling.

- Constants (User-Agents, rate limiting configs)
- Checkpoint management
- Helper functions
"""

import json
from datetime import datetime, UTC
from pathlib import Path
from typing import List, Dict, Tuple

from common.utils.logger import get_logger

logger = get_logger(__name__)

# ============================================================================
# CONSTANTS
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

# Rate limiting
DELAY_MIN = 0.5  # Minimum delay between requests (seconds)
DELAY_MAX = 1.5  # Maximum delay between requests (seconds)

# Retry settings
MAX_RETRIES = 3  # Max retries for 429/503 errors
BACKOFF_BASE = 2  # Exponential backoff base (seconds)

# Checkpoint settings
CHECKPOINT_INTERVAL = 100  # Save checkpoint every N products

# File paths
project_root = Path(__file__).parent.parent.parent
INPUT_FILE = project_root / "data" / "exports" / "product_url_map.csv"
CHECKPOINT_FILE = project_root / "data" / "exports" / "crawl_checkpoint.json"

# URL Cleaning - Tracking parameters to remove
TRACKING_PARAMS = [
    'fbclid', 'utm_source', 'utm_medium', 'utm_campaign', 'utm_content', 'utm_term',
    'itm_source', 'itm_medium', 'itm_campaign', 'itm_content',
    'gclid', 'gclsrc', '_ga', 'mc_cid', 'mc_eid',
    'msclkid', 'zanpid', 'aff_id', 'ref', 'source'
]


# ============================================================================
# URL CLEANING
# ============================================================================

def clean_url(url: str) -> str:
    """
    Remove tracking parameters from URL to reduce WAF detection.
    Tracking parameters (fbclid, utm_*, itm_*, etc.) trigger WAF scoring.

    Example:
        >>> clean_url("https://glamira.com/ring.html?fbclid=123&alloy=gold")
        'https://glamira.com/ring.html?alloy=gold'
    """
    from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

    parsed = urlparse(url)

    # Parse query parameters
    params = parse_qs(parsed.query, keep_blank_values=True)

    # Remove tracking params (keep product params like alloy, stone, diamond)
    cleaned_params = {
        k: v for k, v in params.items()
        if k not in TRACKING_PARAMS
    }

    # Rebuild query string
    new_query = urlencode(cleaned_params, doseq=True) if cleaned_params else ''

    # Rebuild URL
    cleaned = parsed._replace(query=new_query)

    return urlunparse(cleaned)


# ============================================================================
# CHECKPOINT MANAGEMENT
# ============================================================================

def save_checkpoint(checkpoint_file: Path, results: List[Dict]) -> None:
    """
    Save checkpoint with full results for resuming crawl.

    Args:
        checkpoint_file: Path to checkpoint file
        results: List of result dicts
    """
    checkpoint_file.parent.mkdir(parents=True, exist_ok=True)

    checkpoint = {
        "version": "2.0",
        "timestamp": datetime.now(UTC).isoformat(),
        "total_products": len(results),
        "results": results
    }

    with open(checkpoint_file, "w", encoding="utf-8") as f:
        json.dump(checkpoint, f, indent=2, ensure_ascii=False)

    logger.info(f"Checkpoint saved: {len(results)} products")


def load_checkpoint(checkpoint_file: Path) -> List[Dict]:
    """
    Load checkpoint if exists.

    Args:
        checkpoint_file: Path to checkpoint file

    Returns:
        List of result dicts (empty if no checkpoint)
    """
    if not checkpoint_file.exists():
        return []

    try:
        with open(checkpoint_file, "r", encoding="utf-8") as f:
            checkpoint = json.load(f)

        # Handle both v1 and v2 formats
        if "results" in checkpoint:
            results = checkpoint["results"]
        else:
            # Old format compatibility
            results = checkpoint if isinstance(checkpoint, list) else []

        logger.info(f"Loaded checkpoint: {len(results)} products")
        return results

    except (json.JSONDecodeError, Exception) as e:
        logger.warning(f"Failed to load checkpoint: {e}")
        return []


def get_processed_ids(results: List[Dict]) -> set:
    """
    Extract set of processed product IDs from results.

    Args:
        results: List of result dicts

    Returns:
        Set of product_ids
    """
    return {r["product_id"] for r in results if "product_id" in r}


# ============================================================================
# BROWSER HEADERS
# ============================================================================

def get_browser_headers() -> Dict[str, str]:
    """
    Get full browser headers to bypass anti-bot detection.

    Returns:
        Dict of HTTP headers
    """
    return {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webeb,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,pt;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0",
    }
