"""
Dead Letter Queue (DLQ) Retry Module for Failed Products.

Features:
- Retry failed products from previous crawl with lower concurrency
- Special curl_cffi retry for 403 errors (TLS fingerprint spoofing)
- Results merging with priority: curl_cffi > dlq > original
- Failure analysis and reporting

Usage:
    # Standard retry with httpx
    python -m ingestion.product_crawler.retry

    # Retry only 403 errors with curl_cffi
    python -m ingestion.product_crawler.retry --403-only

    # Analyze failures
    python -m ingestion.product_crawler.retry --analyze
"""

import asyncio
import json
import sys
import time
from pathlib import Path
from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import Counter

from common.utils.logger import get_logger
from ingestion.product_crawler import config
from ingestion.product_crawler.crawler import crawl_products_async
from ingestion.product_crawler.parsers import process_html_to_product
from ingestion.product_crawler.utils import (
    clean_url,
    get_browser_headers,
    summarize_results,
)

logger = get_logger(__name__)


# ============================================================================
# RETRY FAILED PRODUCTS (HTTPX)
# ============================================================================

async def retry_failed_products(
    input_file: Path,
    output_file: Path,
    concurrency: int = None
) -> List[Dict]:
    """
    Retry failed products and return merged results.

    Args:
        input_file: Base crawl results JSON (to retry from and merge into)
        output_file: Output merged file with timestamp
        concurrency: Max concurrent requests (default: from config)

    Returns:
        List of merged results (all products with retried ones replaced)
    """
    if concurrency is None:
        concurrency = config.CONCURRENCY_DLQ

    # Load previous results
    if not input_file.exists():
        logger.error(f"Input file not found: {input_file}")
        return []

    with open(input_file, "r", encoding="utf-8") as f:
        all_results = json.load(f)

    logger.info(f"Loaded {len(all_results)} products from previous crawl")

    # Extract failed products
    failed = [r for r in all_results if r.get("status") == "error"]
    logger.info(f"Found {len(failed)} failed products to retry")

    if not failed:
        logger.info("No failed products to retry!")
        return []

    # Show error breakdown
    http_403 = sum(1 for r in failed if r.get("http_status") == 403)
    http_404 = sum(1 for r in failed if r.get("http_status") == 404)
    no_react = sum(1 for r in failed if r.get("status") == "no_react_data")

    logger.info("\nFailed products breakdown:")
    logger.info(f"  HTTP 403: {http_403}")
    logger.info(f"  HTTP 404: {http_404}")
    logger.info(f"  No react_data: {no_react}")
    logger.info("=" * 70)

    # Prepare products for retry (clean URLs)
    products = [(r["product_id"], clean_url(r["url"])) for r in failed]

    # Retry with lower concurrency
    logger.info(f"Retrying {len(products)} failed products (concurrency={concurrency})")
    logger.info("Strategy: Lower concurrency + URL cleaning + HTML fallback parser")

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

    # Calculate stats using helper
    stats = summarize_results(results)

    logger.info("\n" + "=" * 70)
    logger.info("DLQ RETRY SUMMARY")
    logger.info("=" * 70)
    logger.info(f"Total retried: {stats['total']}")
    logger.info(f"Recovered: {stats['success']} ({stats['success_rate']:.1f}%)")
    logger.info(f"Still failed: {stats['errors']}")
    logger.info(f"Total time: {total_time/60:.2f} minutes")
    logger.info(f"Average rate: {stats['total']/total_time:.2f} products/second")
    logger.info("=" * 70)

    # Merge retry results back into all_results
    logger.info("\nMerging retry results...")
    retry_lookup = {str(r["product_id"]): r for r in results}

    merged = []
    replaced_count = 0
    for item in all_results:
        pid = str(item["product_id"])
        if pid in retry_lookup:
            merged.append(retry_lookup[pid])
            replaced_count += 1
        else:
            merged.append(item)

    # Final stats
    final_stats = summarize_results(merged)
    logger.info(f"Replaced {replaced_count} products with retry results")
    logger.info(f"Final success: {final_stats['success']}/{final_stats['total']} ({final_stats['success_rate']:.1f}%)")

    # Save merged results
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(merged, f, indent=2, ensure_ascii=False)
    logger.info(f"\nMerged results saved to: {output_file}")

    return merged


# ============================================================================
# RETRY 403 ERRORS WITH CURL_CFFI
# ============================================================================

def retry_403_with_curlcffi(
    input_file: Path,
    output_file: Path,
    concurrency: int = 3
) -> List[Dict]:
    """
    Retry only 403 errors using curl_cffi and return merged results.

    curl_cffi can bypass WAF detection by impersonating real browser TLS signatures.

    Args:
        input_file: Base results (to retry 403s from and merge into)
        output_file: Output merged file with timestamp
        concurrency: Max workers (default: 3 for safety)

    Returns:
        List of merged results (all products with retried 403s replaced)
    """
    try:
        from curl_cffi import requests as curl_requests
    except ImportError:
        logger.error("curl_cffi not installed! Install with: poetry add curl_cffi")
        sys.exit(1)

    # Load base results
    with open(input_file, "r", encoding="utf-8") as f:
        all_results = json.load(f)

    # Filter only 403 errors
    failed_403 = [
        r for r in all_results
        if r.get("status") == "error" and r.get("http_status") == 403
    ]

    logger.info(f"Found {len(failed_403)} products with HTTP 403 to retry with curl_cffi")

    if not failed_403:
        logger.info("No 403 errors to retry!")
        return []

    results = []
    start = time.time()

    def fetch_one(product_id: str, url: str) -> Dict:
        """Fetch single product with curl_cffi."""
        result = {
            "product_id": product_id,
            "url": url,
            "status": "error",
            "tool": "curl_cffi"
        }

        try:
            resp = curl_requests.get(
                url,
                headers=get_browser_headers(),
                impersonate="chrome120",
                timeout=30
            )
            result["http_status"] = resp.status_code

            if resp.status_code != 200:
                result["error_message"] = f"HTTP {resp.status_code}"
                return result

            # Use unified HTML processor (DRY)
            html = resp.text
            result = process_html_to_product(html, product_id, url)
            result["tool"] = "curl_cffi"  # Mark tool used
            return result

        except Exception as e:
            result["error_message"] = str(e)[:100]
            return result

    # Process with threading
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = {
            executor.submit(fetch_one, r["product_id"], clean_url(r["url"])): r
            for r in failed_403
        }

        for i, future in enumerate(as_completed(futures), 1):
            results.append(future.result())

            if i % 50 == 0 or i == len(failed_403):
                elapsed = time.time() - start
                success = sum(1 for r in results if r["status"] == "success")
                logger.info(
                    f"[{i}/{len(failed_403)}] Recovered: {success} | "
                    f"Elapsed: {elapsed/60:.1f}min"
                )

            # Rate limiting
            time.sleep(2.0)

    # Summary
    stats = summarize_results(results)

    logger.info("\n" + "=" * 70)
    logger.info("CURL_CFFI RETRY SUMMARY")
    logger.info("=" * 70)
    logger.info(f"Total retried: {stats['total']}")
    logger.info(f"Recovered: {stats['success']} ({stats['success_rate']:.1f}%)")
    logger.info("=" * 70)

    # Merge successful curl_cffi results back into all_results
    logger.info("\nMerging curl_cffi results...")
    cffi_lookup = {
        str(r["product_id"]): r
        for r in results
        if r.get("status") == "success"  # Only use successful retries
    }

    merged = []
    replaced_count = 0
    for item in all_results:
        pid = str(item["product_id"])
        if pid in cffi_lookup:
            merged.append(cffi_lookup[pid])
            replaced_count += 1
        else:
            merged.append(item)

    # Final stats
    final_stats = summarize_results(merged)
    logger.info(f"Replaced {replaced_count} products with curl_cffi results")
    logger.info(f"Final success: {final_stats['success']}/{final_stats['total']} ({final_stats['success_rate']:.1f}%)")

    # Save merged results
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(merged, f, indent=2, ensure_ascii=False)
    logger.info(f"\nMerged results saved to: {output_file}")

    return merged


# ============================================================================
# MERGE RESULTS
# ============================================================================

def merge_results(
    original_file: Path,
    retry_file: Path,
    output_file: Path
) -> None:
    """
    Merge original and retry results.

    Replace failed products in original with retry results.

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
    retry_lookup = {str(r["product_id"]): r for r in retry}

    # Merge: replace with retry results if available
    merged = []
    replaced_count = 0

    for original_result in original:
        product_id = str(original_result["product_id"])

        # If this product was retried, use retry result
        if product_id in retry_lookup:
            merged.append(retry_lookup[product_id])
            replaced_count += 1
        else:
            merged.append(original_result)

    # Summary
    stats = summarize_results(merged)

    logger.info("\n" + "=" * 70)
    logger.info("MERGED RESULTS SUMMARY")
    logger.info("=" * 70)
    logger.info(f"Total products: {stats['total']}")
    logger.info(f"Replaced with retry: {replaced_count}")
    logger.info(f"Success: {stats['success']} ({stats['success_rate']:.1f}%)")
    logger.info(f"Errors: {stats['errors']}")
    logger.info("=" * 70)

    # Save merged results
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(merged, f, indent=2, ensure_ascii=False)
    logger.info(f"\nMerged results saved to: {output_file}")


# ============================================================================
# ANALYZE FAILURES
# ============================================================================

def analyze_failures(input_file: Path) -> None:
    """
    Analyze failure patterns in crawl results.

    Args:
        input_file: Crawl results JSON file
    """
    with open(input_file, "r", encoding="utf-8") as f:
        results = json.load(f)

    failed = [r for r in results if r.get("status") == "error"]

    if not failed:
        logger.info("No failures to analyze!")
        return

    logger.info("\n" + "=" * 70)
    logger.info("FAILURE ANALYSIS")
    logger.info("=" * 70)
    logger.info(f"\nTotal failed: {len(failed)}")

    # HTTP Status breakdown
    http_status = Counter(r.get("http_status") for r in failed)
    logger.info(f"\nHTTP Status distribution:")
    for status, count in http_status.most_common():
        logger.info(f"  {status}: {count} ({count/len(failed)*100:.1f}%)")

    # Domain breakdown
    domains = Counter(r["url"].split("/")[2] for r in failed if "url" in r)
    logger.info(f"\nTop 10 domains with failures:")
    for domain, count in domains.most_common(10):
        logger.info(f"  {domain}: {count} ({count/len(failed)*100:.1f}%)")

    # Error messages
    errors = Counter(
        r.get("error_message", "unknown")[:60]
        for r in failed
        if r.get("error_message")
    )
    logger.info(f"\nTop 5 error messages:")
    for msg, count in errors.most_common(5):
        logger.info(f"  [{count}] {msg}")

    logger.info("=" * 70)


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

async def main():
    """Main entry point for DLQ retry module."""
    import argparse

    parser = argparse.ArgumentParser(description="DLQ Retry for Failed Products")
    parser.add_argument(
        "--403-only",
        action="store_true",
        dest="only_403",
        help="Retry only 403 errors with curl_cffi (TLS spoofing)"
    )
    parser.add_argument(
        "--analyze",
        action="store_true",
        help="Analyze failure patterns"
    )
    parser.add_argument(
        "--input",
        type=str,
        help="Input file path (default: full_crawl_results.json)"
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Output file path (default: auto-generated)"
    )

    args = parser.parse_args()

    # Default paths
    input_file = Path(args.input) if args.input else (config.OUTPUT_DIR / "full_crawl_results.json")

    if args.analyze:
        # Analyze mode
        logger.info("FAILURE ANALYSIS MODE")
        logger.info("=" * 70)
        analyze_failures(input_file)
        return

    if args.only_403:
        # Retry 403s with curl_cffi
        logger.info("DLQ Level 2: Retry 403 errors with curl_cffi (TLS spoofing)")
        logger.info("=" * 70)

        # Input: previous retry results
        retry_input = config.OUTPUT_DIR / "retry_failed_results.json"
        output_file = Path(args.output) if args.output else (
            config.OUTPUT_DIR / "retry_curlcffi_results.json"
        )

        results = retry_403_with_curlcffi(retry_input, output_file)

        if results:
            # Merge: original + dlq + curl_cffi
            logger.info("\nMerging original + DLQ + curl_cffi...")

            merged_file = config.OUTPUT_DIR / "full_crawl_results_merged2.json"

            # Load all results
            with open(input_file, "r", encoding="utf-8") as f:
                original = json.load(f)
            with open(retry_input, "r", encoding="utf-8") as f:
                dlq = json.load(f)

            # Create lookups (priority: curl_cffi > dlq > original)
            dlq_map = {str(r["product_id"]): r for r in dlq}
            # Only use successful curl_cffi results
            cffi_map = {
                str(r["product_id"]): r
                for r in results
                if r.get("status") == "success"
            }

            # Merge with priority
            merged = []
            for item in original:
                pid = str(item["product_id"])
                merged.append(cffi_map.get(pid) or dlq_map.get(pid) or item)

            # Save and report
            with open(merged_file, "w", encoding="utf-8") as f:
                json.dump(merged, f, indent=2, ensure_ascii=False)

            success = sum(1 for r in merged if r["status"] == "success")
            logger.info(f"Final: {success}/{len(merged)} = {success/len(merged)*100:.1f}%")
            logger.info(f"Saved: {merged_file}")

    else:
        # Normal retry with httpx
        logger.info("Dead Letter Queue (DLQ) Retry for Failed Products")
        logger.info("=" * 70)

        output_file = Path(args.output) if args.output else (
            config.OUTPUT_DIR / "retry_failed_results.json"
        )

        retry_results = await retry_failed_products(
            input_file=input_file,
            output_file=output_file
        )

        if not retry_results:
            logger.info("No products to retry. Exiting.")
            return

        # Merge results
        logger.info("\nMerging retry results with original crawl...")
        merged_file = config.OUTPUT_DIR / "full_crawl_results_merged.json"
        merge_results(
            original_file=input_file,
            retry_file=output_file,
            output_file=merged_file
        )

        logger.info("\nDLQ Retry complete!")
        logger.info(f"Check merged results: {merged_file}")


if __name__ == "__main__":
    asyncio.run(main())
