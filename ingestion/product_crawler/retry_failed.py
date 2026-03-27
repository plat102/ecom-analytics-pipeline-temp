"""
Dead Letter Queue (DLQ) Retry Script for Failed Products.

Retries only failed products from a previous crawl with settings:
- Lower concurrency (5) for hard cases
- Longer delays (up to 3s) to avoid rate limiting
- URL cleaning enabled
- HTML fallback parser enabled

Usage:
    poetry run python -m ingestion.product_crawler.retry_failed

Input:
    data/exports/full_crawl_results.json (previous crawl results)

Output:
    data/exports/retry_failed_results.json (retry results only)
    data/exports/full_crawl_results_merged.json (merged results)
"""

import asyncio
import json
import sys
from pathlib import Path
from typing import List, Dict

# Add project root
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from common.utils.logger import get_logger
from ingestion.product_crawler.crawler import crawl_products_async
from ingestion.product_crawler.utils import clean_url

logger = get_logger(__name__)

# DLQ-specific settings
DLQ_CONCURRENCY = 5  # Lower concurrency for hard cases
DLQ_DELAY_MIN = 1.0  # Longer delays
DLQ_DELAY_MAX = 3.0

INPUT_FILE = project_root / "data" / "exports" / "full_crawl_results.json"
OUTPUT_FILE = project_root / "data" / "exports" / "retry_failed_results.json"
MERGED_FILE = project_root / "data" / "exports" / "full_crawl_results_merged.json"


async def retry_failed_products(
    input_file: Path,
    output_file: Path,
    concurrency: int = 5
) -> List[Dict]:
    """
    Retry only failed products from previous crawl.

    Args:
        input_file: Path to previous crawl results JSON
        output_file: Path to save retry results
        concurrency: Max concurrent requests (default: 5 for safety)

    Returns:
        List of retry result dicts
    """
    # Load previous results
    if not input_file.exists():
        logger.error(f"Input file not found: {input_file}")
        return []

    with open(input_file, "r", encoding="utf-8") as f:
        all_results = json.load(f)

    logger.info(f"Loaded {len(all_results)} products from previous crawl")

    # Extract failed products (status == "error")
    failed = [r for r in all_results if r.get("status") == "error"]
    logger.info(f"Found {len(failed)} failed products to retry")

    if not failed:
        logger.info("No failed products to retry!")
        return []

    # Extract (product_id, url) tuples
    # Clean URLs to improve success rate
    products = [
        (r["product_id"], clean_url(r["url"]))
        for r in failed
    ]

    # Show error breakdown
    http_403 = sum(1 for r in failed if r.get("http_status") == 403)
    http_404 = sum(1 for r in failed if r.get("http_status") == 404)
    no_react = sum(1 for r in failed if r.get("status") == "no_react_data")

    logger.info("\nFailed products breakdown:")
    logger.info(f"  HTTP 403: {http_403}")
    logger.info(f"  HTTP 404: {http_404}")
    logger.info(f"  No react_data: {no_react}")
    logger.info("=" * 70)

    # Retry with lower concurrency + longer delays
    logger.info(f"Retrying {len(products)} failed products (concurrency={concurrency})")
    logger.info("Using: URL cleaning + HTML fallback parser + longer delays")

    import time
    start_time = time.time()

    # Progress callback
    def progress_callback(completed: int, total: int):
        if completed % 10 == 0 or completed == 1 or completed == total:
            elapsed = time.time() - start_time
            rate = completed / elapsed if elapsed > 0 else 0
            eta = (total - completed) / rate if rate > 0 else 0
            logger.info(
                f"[{completed}/{total}] "
                f"Elapsed: {elapsed/60:.1f}min | "
                f"Rate: {rate:.2f} prod/s | "
                f"ETA: {eta/60:.1f}min"
            )

    # Retry (no checkpoint for DLQ)
    results = await crawl_products_async(
        products,
        concurrency=concurrency,
        progress_callback=progress_callback,
        checkpoint_file=None
    )

    total_time = time.time() - start_time

    # Summary
    success_count = sum(1 for r in results if r["status"] == "success")
    error_count = sum(1 for r in results if r["status"] == "error")
    recovery_rate = success_count / len(results) * 100 if results else 0

    logger.info("\n" + "=" * 70)
    logger.info("DLQ RETRY SUMMARY")
    logger.info("=" * 70)
    logger.info(f"Total retried: {len(results)}")
    logger.info(f"Recovered: {success_count} ({recovery_rate:.1f}%)")
    logger.info(f"Still failed: {error_count}")
    logger.info(f"Total time: {total_time/60:.2f} minutes")
    logger.info(f"Average rate: {len(results)/total_time:.2f} products/second")
    logger.info("=" * 70)

    # Save retry results
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    logger.info(f"\nRetry results saved to: {output_file}")

    return results


def merge_results(
    original_file: Path,
    retry_file: Path,
    output_file: Path
) -> None:
    """
    Merge original and retry results.

    Replace failed products with retry results.

    Args:
        original_file: Original crawl results
        retry_file: Retry results
        output_file: Merged output file
    """
    # Load both files
    with open(original_file, "r", encoding="utf-8") as f:
        original = json.load(f)

    with open(retry_file, "r", encoding="utf-8") as f:
        retry = json.load(f)

    logger.info(f"Merging {len(original)} original + {len(retry)} retry results")

    # Create lookup for retry results by product_id
    retry_lookup = {r["product_id"]: r for r in retry}

    # Merge: replace failed products with retry results
    merged = []
    replaced_count = 0

    for original_result in original:
        product_id = original_result["product_id"]

        # If this product was retried, use retry result
        if product_id in retry_lookup:
            merged.append(retry_lookup[product_id])
            replaced_count += 1
        else:
            merged.append(original_result)

    # Summary
    success_count = sum(1 for r in merged if r["status"] == "success")
    error_count = sum(1 for r in merged if r["status"] == "error")
    success_rate = success_count / len(merged) * 100

    logger.info("\n" + "=" * 70)
    logger.info("MERGED RESULTS SUMMARY")
    logger.info("=" * 70)
    logger.info(f"Total products: {len(merged)}")
    logger.info(f"Replaced with retry: {replaced_count}")
    logger.info(f"Success: {success_count} ({success_rate:.1f}%)")
    logger.info(f"Errors: {error_count}")
    logger.info("=" * 70)

    # Save merged results
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(merged, f, indent=2, ensure_ascii=False)
    logger.info(f"\nMerged results saved to: {output_file}")


async def main():
    """Main entry point for DLQ retry."""
    logger.info("Dead Letter Queue (DLQ) Retry for Failed Products")
    logger.info("=" * 70)

    # Retry failed products
    retry_results = await retry_failed_products(
        input_file=INPUT_FILE,
        output_file=OUTPUT_FILE,
        concurrency=DLQ_CONCURRENCY
    )

    if not retry_results:
        logger.info("No products to retry. Exiting.")
        return

    # Merge with original results
    logger.info("\nMerging retry results with original crawl...")
    merge_results(
        original_file=INPUT_FILE,
        retry_file=OUTPUT_FILE,
        output_file=MERGED_FILE
    )

    logger.info("\nDLQ Retry complete!")
    logger.info(f"Check merged results: {MERGED_FILE}")


if __name__ == "__main__":
    asyncio.run(main())
