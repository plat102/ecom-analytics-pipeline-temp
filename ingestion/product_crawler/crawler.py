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
from datetime import date
from pathlib import Path
from typing import List, Tuple, Dict, Optional, Callable

import httpx
from common.utils.logger import get_logger

# Import config
from ingestion.product_crawler import config
from ingestion.product_crawler.parsers import process_html_to_product
from ingestion.product_crawler.utils import (
    save_checkpoint,
    load_checkpoint,
    get_processed_ids,
    get_browser_headers,
    clean_url,
    summarize_results,
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
        user_agent = random.choice(config.USER_AGENTS)

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

    Uses while loop with flag-based backoff to avoid semaphore deadlock.

    Args:
        client: Shared AsyncClient
        product_id: Product ID
        url: Product URL
        semaphore: Semaphore for concurrency control

    Returns:
        Dict with product data or error info
    """
    # Clean URL first - remove tracking params (fbclid, utm_*, itm_*)
    url = clean_url(url)

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
    while retry_count <= config.MAX_RETRIES:
        need_backoff = False
        backoff_time = 0

        async with semaphore:  # Acquire semaphore for each attempt
            # Fetch HTML
            html, status_code = await fetch_html_async(client, url)
            result["http_status"] = status_code

            # Handle retryable errors (403/429/503) -> Mark for backoff
            if status_code in [403, 429, 503] and retry_count < config.MAX_RETRIES:
                retry_count += 1
                backoff_time = config.BACKOFF_BASE ** retry_count
                need_backoff = True

                # Special handling for 403: Switch to clean catalog URL (bypass WAF)
                if status_code == 403:
                    catalog_url = f"https://www.glamira.com/catalog/product/view/id/{product_id}"
                    logger.info(
                        f"Product {product_id}: 403 blocked, "
                        f"switching to catalog URL for retry {retry_count}/{config.MAX_RETRIES}"
                    )
                    url = catalog_url  # Reassign URL for next iteration
                    result["fallback_used"] = True

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

            # Non-retryable error or max retries exceeded
            elif html is None:
                if status_code in [403, 429, 503]:
                    result["error_message"] = f"Rate limited/Forbidden after {config.MAX_RETRIES} attempts"
                else:
                    result["error_message"] = f"Failed to fetch HTML (HTTP {status_code})"
                return result

            # Extract data (only if we have HTML and not flagged for backoff)
            if html and not need_backoff:
                # Use unified HTML processor (DRY)
                result = process_html_to_product(html, product_id, url)

                # Random delay to mimic human behavior (inside semaphore to maintain rate limit)
                delay = random.uniform(config.DELAY_MIN, config.DELAY_MAX)
                await asyncio.sleep(delay)

                return result

        # --- EXIT SEMAPHORE BLOCK ---
        # Perform backoff sleep WITHOUT holding semaphore
        if need_backoff:
            error_names = {403: "Forbidden", 429: "Rate limited", 503: "Server error"}
            logger.warning(
                f"{error_names.get(status_code, 'Error')} ({status_code}) for product {product_id}, "
                f"retry {retry_count}/{config.MAX_RETRIES} after {backoff_time}s"
            )
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
            if checkpoint_file and i % config.CHECKPOINT_INTERVAL == 0:
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
# UNIFIED CRAWL FUNCTION
# ============================================================================

async def run_crawl(
    input_file: Path = None,
    output_file: Path = None,
    concurrency: int = None,
    limit: Optional[int] = None,
    resume: bool = False,
    checkpoint: bool = True
) -> int:
    """
    Unified crawl function for both test and full crawl modes.

    Args:
        input_file: CSV file with product_id and url columns (default: from config)
        output_file: Output JSON file (default: deterministic based on mode/date)
        concurrency: Max concurrent requests (default: from config based on mode)
        limit: Number of products to crawl (None = all products for full crawl)
        resume: Resume from checkpoint (skip already processed products)
        checkpoint: Enable checkpointing

    Returns:
        Exit code (0=success, 1=warning, 2=failure)
    """
    import csv

    # Defaults
    if input_file is None:
        input_file = config.INPUT_FILE

    if concurrency is None:
        concurrency = config.CONCURRENCY_TEST if limit else config.CONCURRENCY_FULL

    # Determine mode
    is_test = limit is not None
    mode_name = f"TEST ({limit} products)" if is_test else "FULL CRAWL"

    # Load products from CSV
    if not input_file.exists():
        logger.error(f"Input file not found: {input_file}")
        return 1

    logger.info(f"{mode_name}: Loading products from {input_file}")

    products = []
    with open(input_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if limit and i >= limit:
                break
            products.append((row["product_id"], row["url"]))

    # Handle resume mode
    if resume:
        checkpoint_file = config.CHECKPOINT_FILE if checkpoint else None
        if checkpoint_file and checkpoint_file.exists():
            logger.info("Resume mode: Loading checkpoint...")
            previous_results = load_checkpoint(checkpoint_file)
            processed_ids = get_processed_ids(previous_results)

            # Filter out already processed products
            products = [(pid, url) for pid, url in products if pid not in processed_ids]

            logger.info(f"Resume: {len(processed_ids)} already processed, {len(products)} remaining")

            if not products:
                logger.info("All products already processed!")
                return 0
        else:
            logger.warning("Resume mode requested but no checkpoint found, starting fresh")

    logger.info(f"Crawling {len(products)} products with concurrency={concurrency}")
    logger.info("=" * 70)

    # Progress callback
    start_time = time.time()
    progress_interval = 10 if is_test else 100

    def progress_callback(completed: int, total: int):
        if completed % progress_interval == 0 or completed == 1:
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
    checkpoint_file = config.CHECKPOINT_FILE if checkpoint else None
    results = await crawl_products_async(
        products,
        concurrency=concurrency,
        progress_callback=progress_callback,
        checkpoint_file=checkpoint_file
    )

    # Calculate stats using helper
    total_time = time.time() - start_time
    stats = summarize_results(results)

    # Summary
    logger.info("\n" + "=" * 70)
    logger.info(f"{mode_name} SUMMARY")
    logger.info("=" * 70)
    logger.info(f"Total crawled: {stats['total']}")
    logger.info(f"Success: {stats['success']} ({stats['success_rate']:.1f}%)")
    logger.info(f"Errors: {stats['errors']}")
    logger.info(f"No react_data: {stats['no_react_data']}")
    logger.info(f"HTTP 403: {stats['http_403']}")
    logger.info(f"HTTP 404 (fallback used): {stats['fallback_used']}")
    logger.info(f"Total time: {total_time/60:.2f} minutes ({total_time/3600:.2f} hours)")
    logger.info(f"Average rate: {stats['total']/total_time:.2f} products/second")

    # Projection for test mode
    if is_test and stats['total'] > 0:
        total_products = 19417  # Approximate total from full dataset
        projected_time = total_products / (stats['total'] / total_time)
        logger.info(f"\nProjected time for {total_products} products:")
        logger.info(f"  ~{projected_time/3600:.1f} hours ({projected_time/60:.0f} minutes)")

    logger.info("=" * 70)

    # Determine output file (deterministic)
    if output_file is None:
        if is_test:
            # Test mode: include date for traceability
            today = date.today().strftime("%Y%m%d")
            output_file = config.OUTPUT_DIR / f"test_{limit}_results_{today}.json"
        else:
            # Full crawl: fixed name (overwrite)
            output_file = config.OUTPUT_DIR / "full_crawl_results.json"

    # Save results
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    logger.info(f"\nResults saved to: {output_file}")

    # Exit code based on success rate
    if stats['success_rate'] >= 90:
        logger.info(f"\nCRAWL PASSED: {stats['success_rate']:.1f}% success rate")
        return 0
    elif stats['success_rate'] >= 80:
        logger.warning(f"\nCRAWL WARNING: {stats['success_rate']:.1f}% success rate")
        return 1
    else:
        logger.error(f"\nCRAWL FAILED: {stats['success_rate']:.1f}% success rate")
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
        help="Resume from checkpoint (skip already processed products)"
    )
    parser.add_argument(
        "--no-checkpoint",
        action="store_true",
        help="Disable checkpointing"
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Custom output file path"
    )

    args = parser.parse_args()

    # Parse output file
    output_file = Path(args.output) if args.output else None

    # Determine mode
    if args.test is not None:
        # Test mode
        count = args.test if args.test > 0 else 30
        checkpoint = not args.no_checkpoint
        logger.info(f"Test mode: {count} products, concurrency={args.concurrency or 'auto'}")
        exit_code = asyncio.run(run_crawl(
            limit=count,
            concurrency=args.concurrency,
            resume=args.resume,
            checkpoint=checkpoint,
            output_file=output_file
        ))
        sys.exit(exit_code)
    else:
        # Full crawl mode (default)
        logger.info(f"Full crawl mode: ALL products, concurrency={args.concurrency or 'auto'}")
        exit_code = asyncio.run(run_crawl(
            limit=None,
            concurrency=args.concurrency,
            resume=args.resume,
            checkpoint=not args.no_checkpoint,
            output_file=output_file
        ))
        sys.exit(exit_code)


if __name__ == "__main__":
    main()
