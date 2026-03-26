"""
Async product data extraction using httpx + react_data parsing.

Features:
- Async/await with httpx.AsyncClient for true concurrency
- Connection pooling and reuse (HTTP/2 multiplexing)
- Configurable concurrency with semaphore (default: 20 test, 15 full)
- Exponential backoff for rate limiting (429/503 errors)
- 404 fallback to stable catalog endpoint
- Random delays to mimic human behavior
- Progress tracking and checkpointing
- Error rate monitoring
"""

import asyncio
import json
import random
import sys
import time
from pathlib import Path
from typing import List, Tuple, Dict, Optional, Callable

# Add project root
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import httpx
from common.utils.logger import get_logger
from ingestion.product_crawler.parsers import extract_react_data, extract_product_fields
from ingestion.product_crawler.utils import (
    USER_AGENTS,
    DELAY_MIN,
    DELAY_MAX,
    MAX_RETRIES,
    BACKOFF_BASE,
    CHECKPOINT_INTERVAL,
    INPUT_FILE,
    CHECKPOINT_FILE,
    save_checkpoint,
    get_browser_headers,
)

logger = get_logger(__name__)

# ============================================================================
# CORE ASYNC FUNCTIONS
# ============================================================================

async def fetch_html_async(
    client: httpx.AsyncClient,
    url: str,
    user_agent: Optional[str] = None
) -> Tuple[Optional[str], Optional[int]]:
    """
    Fetch HTML using async httpx with HTTP/2 and full browser headers.

    Args:
        client: Shared AsyncClient instance
        url: URL to fetch
        user_agent: Custom user agent (default: random from pool)

    Returns:
        Tuple of (html_content, status_code) or (None, status_code) on error
    """
    if user_agent is None:
        user_agent = random.choice(USER_AGENTS)

    # Full browser headers to bypass anti-bot
    headers = {"User-Agent": user_agent, **get_browser_headers()}

    try:
        response = await client.get(url, headers=headers)

        if response.status_code == 200:
            return response.text, 200
        else:
            logger.warning(f"HTTP {response.status_code} for URL: {url}")
            return None, response.status_code

    except httpx.TimeoutException:
        logger.error(f"Timeout fetching URL: {url}")
        return None, None
    except httpx.RequestError as e:
        logger.error(f"Request error for {url}: {e}")
        return None, None
    except Exception as e:
        logger.error(f"Unexpected error for {url}: {e}")
        return None, None


async def fetch_product_async(
    client: httpx.AsyncClient,
    product_id: str,
    url: str,
    semaphore: asyncio.Semaphore
) -> Dict:
    """
    Fetch single product with retry logic, 404 fallback, and rate limiting.

    Uses while loop instead of recursion to avoid semaphore deadlock.

    Args:
        client: Shared AsyncClient
        product_id: Product ID
        url: Product URL
        semaphore: Semaphore for concurrency control

    Returns:
        Dict with product data or error info
    """
    result = {
        "product_id": product_id,
        "url": url,
        "status": "error",
        "http_status": None,
        "error_message": None,
        "fallback_used": False,
    }

    retry_count = 0

    # Retry loop - avoids semaphore deadlock from recursion
    while retry_count <= MAX_RETRIES:
        async with semaphore:  # Acquire semaphore for each attempt
            # Fetch HTML
            html, status_code = await fetch_html_async(client, url)
            result["http_status"] = status_code

            # Handle retryable errors (403/429/503) with exponential backoff
            if status_code in [403, 429, 503] and retry_count < MAX_RETRIES:
                retry_count += 1
                backoff_time = BACKOFF_BASE ** retry_count
                error_names = {403: "Forbidden", 429: "Rate limited", 503: "Server error"}
                logger.warning(
                    f"{error_names[status_code]} ({status_code}) for product {product_id}, "
                    f"retry {retry_count}/{MAX_RETRIES} after {backoff_time}s"
                )
                # Sleep OUTSIDE semaphore (after releasing it)

            # Handle 404 with catalog fallback
            elif status_code == 404:
                catalog_url = f"https://www.glamira.com/catalog/product/view/id/{product_id}"
                logger.info(f"Product {product_id}: 404, trying catalog URL")

                html, status_code = await fetch_html_async(client, catalog_url)
                result["http_status"] = status_code
                result["fallback_used"] = True

                if html is None:
                    result["error_message"] = f"Failed from both URLs (HTTP {status_code})"
                    return result
                # Continue to extract data from fallback

            elif html is None:
                # Non-retryable error or max retries exceeded
                if status_code in [403, 429, 503]:
                    error_msgs = {
                        403: "Forbidden after retries (WAF/IP blocking)",
                        429: "Rate limited after retries",
                        503: "Server error after retries"
                    }
                    result["error_message"] = f"{error_msgs[status_code]} ({MAX_RETRIES} attempts)"
                else:
                    result["error_message"] = f"Failed to fetch HTML (HTTP {status_code})"
                return result

            # If we have HTML, try to extract data
            if html:
                # Extract react_data
                react_data = extract_react_data(html)

                if react_data is None:
                    result["status"] = "no_react_data"
                    result["error_message"] = "react_data not found in HTML"
                    return result

                # Extract product fields
                result["status"] = "success"
                fields = extract_product_fields(react_data)
                result.update(fields)

                # Preserve input product_id (database ID is source of truth)
                result["product_id"] = product_id

                # Random delay to mimic human behavior
                delay = random.uniform(DELAY_MIN, DELAY_MAX)
                await asyncio.sleep(delay)

                return result

        # Sleep OUTSIDE semaphore to avoid blocking
        if retry_count > 0 and retry_count <= MAX_RETRIES and result["status"] == "error":
            backoff_time = BACKOFF_BASE ** retry_count
            await asyncio.sleep(backoff_time)

    # Max retries exceeded
    return result


# ============================================================================
# MAIN CRAWLER
# ============================================================================

async def crawl_products_async(
    products: List[Tuple[str, str]],
    concurrency: int = 20,
    progress_callback: Optional[Callable[[int, int], None]] = None,
    checkpoint_file: Optional[Path] = None
) -> List[Dict]:
    """
    Crawl products asynchronously with connection pooling and progress tracking.

    Args:
        products: List of (product_id, url) tuples
        concurrency: Max concurrent requests (default: 20)
        progress_callback: Optional callback(completed, total) for progress
        checkpoint_file: Path to checkpoint file (for resuming)

    Returns:
        List of result dicts
    """
    logger.info(f"Starting async crawl: {len(products)} products, concurrency={concurrency}")

    # Create shared async client with connection pooling
    async with httpx.AsyncClient(
        http2=True,
        timeout=30.0,
        follow_redirects=True,
        limits=httpx.Limits(
            max_connections=concurrency,
            max_keepalive_connections=concurrency
        )
    ) as client:
        semaphore = asyncio.Semaphore(concurrency)

        # Create tasks
        tasks = [
            fetch_product_async(client, product_id, url, semaphore)
            for product_id, url in products
        ]

        # Process with progress tracking
        results = []
        start_time = time.time()

        for i, coro in enumerate(asyncio.as_completed(tasks), 1):
            result = await coro
            results.append(result)

            # Progress callback
            if progress_callback:
                progress_callback(i, len(products))

            # Checkpoint every N products
            if checkpoint_file and i % CHECKPOINT_INTERVAL == 0:
                save_checkpoint(checkpoint_file, results)
                elapsed = time.time() - start_time
                rate = i / elapsed
                eta = (len(products) - i) / rate if rate > 0 else 0
                logger.info(
                    f"Progress: {i}/{len(products)} | "
                    f"Rate: {rate:.2f} prod/s | "
                    f"ETA: {eta/60:.1f}min"
                )

        total_time = time.time() - start_time
        logger.info(f"Crawl complete: {len(results)} products in {total_time/60:.2f} minutes")

        return results


# ============================================================================
# TEST MODE
# ============================================================================

async def test_mode_async(
    count: int = 30,
    concurrency: int = 20,
    checkpoint: bool = True
) -> int:
    """
    Test mode: Crawl N products from CSV with progress tracking.

    Args:
        count: Number of products to test
        concurrency: Max concurrent requests
        checkpoint: Enable checkpointing

    Returns:
        Exit code (0=success, 1=warning, 2=failure)
    """
    import csv

    # Load products from CSV
    if not INPUT_FILE.exists():
        logger.error(f"Input file not found: {INPUT_FILE}")
        return 1

    logger.info(f"Loading {count} URLs from: {INPUT_FILE}")

    products = []
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if i >= count:
                break
            products.append((row["product_id"], row["url"]))

    logger.info(f"Testing {len(products)} products with {concurrency} concurrent requests")
    logger.info("=" * 70)

    # Progress callback
    start_time = time.time()

    def progress_callback(completed: int, total: int):
        if completed % 10 == 0 or completed == 1:
            elapsed = time.time() - start_time
            rate = completed / elapsed if elapsed > 0 else 0
            eta = (total - completed) / rate if rate > 0 else 0
            logger.info(
                f"[{completed}/{total}] "
                f"Elapsed: {elapsed/60:.1f}min | "
                f"Rate: {rate:.2f} prod/s | "
                f"ETA: {eta/60:.1f}min"
            )

    # Crawl
    checkpoint_file = CHECKPOINT_FILE if checkpoint else None
    results = await crawl_products_async(
        products,
        concurrency=concurrency,
        progress_callback=progress_callback,
        checkpoint_file=checkpoint_file
    )

    # Summary
    total_time = time.time() - start_time
    success_count = sum(1 for r in results if r["status"] == "success")
    error_count = sum(1 for r in results if r["status"] == "error")
    no_react_count = sum(1 for r in results if r["status"] == "no_react_data")
    http_403 = sum(1 for r in results if r.get("http_status") == 403)
    fallback_count = sum(1 for r in results if r.get("fallback_used"))

    logger.info("\n" + "=" * 70)
    logger.info("TEST SUMMARY")
    logger.info("=" * 70)
    logger.info(f"Total tested: {len(results)}")
    logger.info(f"Success: {success_count} ({success_count/len(results)*100:.1f}%)")
    logger.info(f"Errors: {error_count}")
    logger.info(f"No react_data: {no_react_count}")
    logger.info(f"HTTP 403: {http_403}")
    logger.info(f"HTTP 404 (fallback used): {fallback_count}")
    logger.info(f"Total time: {total_time/60:.2f} minutes")
    logger.info(f"Average rate: {len(results)/total_time:.2f} products/second")

    # Projected time for full crawl
    total_products = 19417
    projected_time = total_products / (len(results) / total_time)
    logger.info(f"\nProjected time for {total_products} products:")
    logger.info(f"  ~{projected_time/3600:.1f} hours ({projected_time/60:.0f} minutes)")
    logger.info("=" * 70)

    # Save results
    from pathlib import Path
    project_root = Path(__file__).parent.parent.parent
    output_file = project_root / "data" / "exports" / f"test_{count}_async_results.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    logger.info(f"\nResults saved to: {output_file}")

    # Exit code based on success rate
    success_rate = success_count / len(results) * 100
    if success_rate >= 90:
        logger.info(f"\nTEST PASSED: {success_rate:.1f}% success rate")
        return 0
    elif success_rate >= 80:
        logger.warning(f"\nTEST WARNING: {success_rate:.1f}% success rate")
        return 1
    else:
        logger.error(f"\nTEST FAILED: {success_rate:.1f}% success rate")
        return 2


async def full_crawl_async(concurrency: int = 15) -> int:
    """
    Full crawl mode: Crawl ALL products from CSV.

    Args:
        concurrency: Max concurrent requests (default: 15 for production safety)

    Returns:
        Exit code (0=success, 1=warning, 2=failure)
    """
    import csv

    # Load ALL products from CSV
    if not INPUT_FILE.exists():
        logger.error(f"Input file not found: {INPUT_FILE}")
        return 1

    logger.info(f"Loading ALL products from: {INPUT_FILE}")

    products = []
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            products.append((row["product_id"], row["url"]))

    total_count = len(products)
    logger.info(f"Loaded {total_count} products")
    logger.info("=" * 70)

    # Progress callback
    start_time = time.time()

    def progress_callback(completed: int, total: int):
        if completed % 100 == 0 or completed == 1:
            elapsed = time.time() - start_time
            rate = completed / elapsed if elapsed > 0 else 0
            eta = (total - completed) / rate if rate > 0 else 0
            logger.info(
                f"[{completed}/{total}] "
                f"Elapsed: {elapsed/60:.1f}min | "
                f"Rate: {rate:.2f} prod/s | "
                f"ETA: {eta/60:.1f}min"
            )

    # Crawl with checkpointing
    results = await crawl_products_async(
        products,
        concurrency=concurrency,
        progress_callback=progress_callback,
        checkpoint_file=CHECKPOINT_FILE
    )

    # Summary
    total_time = time.time() - start_time
    success_count = sum(1 for r in results if r["status"] == "success")
    error_count = sum(1 for r in results if r["status"] == "error")
    no_react_count = sum(1 for r in results if r["status"] == "no_react_data")
    http_403 = sum(1 for r in results if r.get("http_status") == 403)
    fallback_count = sum(1 for r in results if r.get("fallback_used"))

    logger.info("\n" + "=" * 70)
    logger.info("FULL CRAWL SUMMARY")
    logger.info("=" * 70)
    logger.info(f"Total crawled: {len(results)}")
    logger.info(f"Success: {success_count} ({success_count/len(results)*100:.1f}%)")
    logger.info(f"Errors: {error_count}")
    logger.info(f"No react_data: {no_react_count}")
    logger.info(f"HTTP 403: {http_403}")
    logger.info(f"HTTP 404 (fallback used): {fallback_count}")
    logger.info(f"Total time: {total_time/60:.2f} minutes ({total_time/3600:.2f} hours)")
    logger.info(f"Average rate: {len(results)/total_time:.2f} products/second")
    logger.info("=" * 70)

    # Save results
    from pathlib import Path
    project_root = Path(__file__).parent.parent.parent
    output_file = project_root / "data" / "exports" / "full_crawl_results.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    logger.info(f"\nResults saved to: {output_file}")

    # Exit code based on success rate
    success_rate = success_count / len(results) * 100
    if success_rate >= 90:
        logger.info(f"\nCRAWL PASSED: {success_rate:.1f}% success rate")
        return 0
    elif success_rate >= 80:
        logger.warning(f"\nCRAWL WARNING: {success_rate:.1f}% success rate")
        return 1
    else:
        logger.error(f"\nCRAWL FAILED: {success_rate:.1f}% success rate")
        return 2


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

def main():
    """Main entry point for async crawler."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Async Glamira product crawler with httpx"
    )
    parser.add_argument(
        "--test",
        type=int,
        metavar="N",
        help="Test mode: crawl N products (e.g., --test 50)"
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Full crawl: crawl ALL products from CSV (default behavior)"
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=None,
        help="Max concurrent requests (default: 20 for test, 15 for full)"
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from checkpoint (not yet implemented)"
    )
    parser.add_argument(
        "--no-checkpoint",
        action="store_true",
        help="Disable checkpointing (only for test mode)"
    )

    args = parser.parse_args()

    # Determine mode
    if args.test is not None:
        # Test mode
        count = args.test if args.test > 0 else 30
        concurrency = args.concurrency if args.concurrency else 20
        checkpoint = not args.no_checkpoint
        logger.info(f"Test mode: {count} products, concurrency={concurrency}")
        exit_code = asyncio.run(test_mode_async(count, concurrency, checkpoint))
        sys.exit(exit_code)
    elif args.resume:
        # Resume from checkpoint (not yet implemented)
        logger.error("Resume mode not yet implemented")
        sys.exit(1)
    else:
        # Full crawl mode (default)
        concurrency = args.concurrency if args.concurrency else 15
        logger.info(f"Full crawl mode: ALL products, concurrency={concurrency}")
        exit_code = asyncio.run(full_crawl_async(concurrency))
        sys.exit(exit_code)


if __name__ == "__main__":
    main()
