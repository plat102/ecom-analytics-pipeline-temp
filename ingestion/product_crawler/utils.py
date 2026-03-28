"""
Shared utilities for product crawling.

- Data processing helpers
- Checkpoint management
- URL cleaning and headers
"""

import json
from datetime import datetime, UTC
from pathlib import Path
from typing import List, Dict, Optional

from common.utils.logger import get_logger
from ingestion.product_crawler.config import TRACKING_PARAMS

logger = get_logger(__name__)


# ============================================================================
# DATA PROCESSING HELPERS
# ============================================================================

def summarize_results(results: List[Dict]) -> Dict:
    """
    Calculate summary statistics for crawl results.

    DRY helper to avoid duplicate stats calculation across test/full/retry modes.

    Args:
        results: List of result dicts from crawler

    Returns:
        Dict with summary statistics:
        - total: total products
        - success: successful extractions
        - success_rate: success percentage
        - errors: error count
        - no_react_data: products without react_data
        - http_403: HTTP 403 count
        - fallback_used: count using fallback URL
    """
    total = len(results)

    if total == 0:
        return {
            "total": 0,
            "success": 0,
            "success_rate": 0.0,
            "errors": 0,
            "no_react_data": 0,
            "http_403": 0,
            "fallback_used": 0
        }

    success = sum(1 for r in results if r.get("status") == "success")

    return {
        "total": total,
        "success": success,
        "success_rate": (success / total * 100) if total > 0 else 0,
        "errors": sum(1 for r in results if r.get("status") == "error"),
        "no_react_data": sum(1 for r in results if r.get("status") == "no_react_data"),
        "http_403": sum(1 for r in results if r.get("http_status") == 403),
        "fallback_used": sum(1 for r in results if r.get("fallback_used"))
    }


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
