"""
Shared utilities for product crawling.
- Fetching (Playwright)
- Checkpoint management
- Error logging

Note: Parsing functions moved to parsers.py
"""

import json
import time
from datetime import datetime, UTC
from pathlib import Path

from playwright.sync_api import Page

from common.utils.logger import get_logger

logger = get_logger(__name__)

# Crawler settings
PAGE_TIMEOUT = 60000  # 60 seconds
MAX_RETRIES = 3
CHECKPOINT_INTERVAL = 100  # Save checkpoint every N products


def log_error(error_file: Path, product_id: str, url: str, error_type: str, **kwargs):
    """Append error to JSONL log."""
    error_record = {
        "product_id": product_id,
        "url": url,
        "error_type": error_type,
        "timestamp": datetime.now(UTC).isoformat(),
        **kwargs
    }

    error_file.parent.mkdir(parents=True, exist_ok=True)
    with open(error_file, 'a', encoding='utf-8') as f:
        f.write(json.dumps(error_record, ensure_ascii=False) + '\n')


def save_checkpoint(checkpoint_file: Path, processed_ids: set, count: int, mode: str = "normal"):
    """Save checkpoint with set of processed product IDs."""
    checkpoint = {
        "processed_ids": list(processed_ids),
        "processed_count": count,
        "timestamp": datetime.now(UTC).isoformat(),
        "mode": mode
    }

    checkpoint_file.parent.mkdir(parents=True, exist_ok=True)
    with open(checkpoint_file, 'w', encoding='utf-8') as f:
        json.dump(checkpoint, f, indent=2)


def load_checkpoint(checkpoint_file: Path):
    """Load checkpoint with set of processed product IDs."""
    if not checkpoint_file.exists():
        return set(), 0

    try:
        with open(checkpoint_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            processed_ids = set(data.get("processed_ids", []))
            count = data.get("processed_count", 0)
            return processed_ids, count
    except (json.JSONDecodeError, Exception) as e:
        logger.warning(f"Failed to load checkpoint: {e}")
        return set(), 0


def fetch_html_with_playwright(page: Page, url: str, product_id: str, error_file: Path):
    """
    Fetch HTML using Playwright with retry logic and HTTP status checking.

    Args:
        page: Playwright Page instance
        url: URL to fetch
        product_id: Product ID for logging
        error_file: Path to error log file

    Returns:
        str | None: HTML content or None if failed
    """
    for attempt in range(MAX_RETRIES):
        try:
            response = page.goto(url, timeout=PAGE_TIMEOUT, wait_until="domcontentloaded")

            if response:
                status = response.status

                if status == 200:
                    return page.content()

                elif status == 404:
                    # Not found - no point retrying
                    log_error(error_file, product_id, url, "HTTPError", status=404, message="Not Found")
                    logger.warning(f"HTTP 404 for {product_id} - not found")
                    return None

                elif status in [403, 429, 500, 502, 503]:
                    # Retry-able errors (rate limit, server errors, forbidden)
                    if attempt < MAX_RETRIES - 1:
                        wait = 2 ** attempt
                        logger.warning(f"HTTP {status} for {product_id}, retry {attempt + 1}/{MAX_RETRIES} in {wait}s")
                        time.sleep(wait)
                        continue
                    else:
                        # Final attempt failed
                        log_error(error_file, product_id, url, "HTTPError", status=status, message="Max retries exceeded")
                        logger.warning(f"HTTP {status} for {product_id} - max retries exceeded")
                        return None

                else:
                    log_error(error_file, product_id, url, "HTTPError", status=status)
                    logger.warning(f"HTTP {status} for {product_id}")
                    return None

        except Exception as e:
            error_msg = str(e)

            if attempt < MAX_RETRIES - 1:
                wait = 2 ** attempt
                logger.warning(f"Error for {product_id} (attempt {attempt + 1}/{MAX_RETRIES}): {error_msg[:100]}, retry in {wait}s")
                time.sleep(wait)
                continue
            else:
                if "Timeout" in error_msg:
                    log_error(error_file, product_id, url, "Timeout", message=error_msg)
                elif "net::ERR" in error_msg:
                    log_error(error_file, product_id, url, "NetworkError", message=error_msg)
                else:
                    log_error(error_file, product_id, url, "UnknownError", message=error_msg)

                logger.warning(f"Failed for {product_id} after {MAX_RETRIES} attempts: {error_msg[:100]}")
                return None

    return None
