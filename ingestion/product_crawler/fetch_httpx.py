"""
Fast product data extraction using httpx + react_data parsing.
- User-Agent rotation (12 different browsers)
- Random delays (1-3s) to avoid pattern detection
- Sequential crawling to stay under rate limits

Usage as module:
    from ingestion.product_crawler.fetch_httpx import fetch_product_httpx
    result = fetch_product_httpx(
        product_id="99021",
        url="https://www.glamira.pt/glamira-ring-amazing-trust-5-mm.html"
    )

Usage as script (test mode):
    # Test 20 products (default)
    poetry run python -m ingestion.product_crawler.fetch_httpx --test 20

    # Test single URL
    poetry run python -m ingestion.product_crawler.fetch_httpx \
        --product-id 99021 \
        --url "https://www.glamira.pt/glamira-ring-amazing-trust-5-mm.html"

    # Quick test (no args)
    poetry run python -m ingestion.product_crawler.fetch_httpx
"""

import json
import random
import re
import sys
from pathlib import Path

# Add project root
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import httpx
from common.utils.logger import get_logger
from ingestion.product_crawler.parsers import extract_react_data, extract_product_fields

logger = get_logger(__name__)

# User-Agent pool for rotation (simulating different browsers)
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


def fetch_html_with_httpx(
    url: str, timeout: int = 30, user_agent: str = None
) -> tuple[str | None, int | None]:
    """
    Fetch HTML using httpx (no browser needed).

    Args:
        url: Product URL
        timeout: Request timeout in seconds
        user_agent: Custom user agent (default: random from USER_AGENTS pool)

    Returns:
        tuple: (html_content, status_code) or (None, status_code) on error
    """
    if user_agent is None:
        user_agent = random.choice(USER_AGENTS)

    try:
        with httpx.Client(timeout=timeout, follow_redirects=True) as client:
            response = client.get(url, headers={"User-Agent": user_agent})

            if response.status_code == 200:
                return response.text, 200
            else:
                logger.warning(f"HTTP {response.status_code} for URL: {url}")
                return None, response.status_code

    except httpx.TimeoutException:
        logger.error(f"Timeout fetching URL: {url}")
        return None, None
    except httpx.RequestError as e:
        logger.error(f"Request error: {e}")
        return None, None
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return None, None


def fetch_product_httpx(product_id: str, url: str, timeout: int = 30) -> dict:
    """
    Fetch product data using httpx + react_data extraction.

    This is the main high-level function for httpx-based crawling.

    Args:
        product_id: Product ID from database
        url: Product URL
        timeout: Request timeout in seconds

    Returns:
        dict: Result with structure:
            - Metadata fields (always present):
                'product_id': str (from input, not react_data)
                'url': str
                'status': 'success' | 'error' | 'no_react_data'
                'http_status': int | None
                'error_message': str | None

            - Product fields (if status='success', from extract_product_fields()):
                43 fields including product_id, product_name, sku, price,
                min_price, max_price, category, category_name, etc.
    """
    result = {
        "product_id": product_id,
        "url": url,
        "status": "error",
        "http_status": None,
        "error_message": None,
    }

    # Fetch HTML
    html, status_code = fetch_html_with_httpx(url, timeout=timeout)
    result["http_status"] = status_code

    if html is None:
        result["error_message"] = f"Failed to fetch HTML (HTTP {status_code})"
        return result

    # Extract react_data
    react_data = extract_react_data(html)

    if react_data is None:
        result["status"] = "no_react_data"
        result["error_message"] = "react_data not found in HTML"
        return result

    # Extract product fields using parser
    result["status"] = "success"
    fields = extract_product_fields(react_data)
    result.update(fields)

    # Keep input product_id (database ID is source of truth)
    result["product_id"] = product_id

    return result


def fetch_product_batch(
    products: list[tuple[str, str]], max_workers: int = 50, timeout: int = 30
) -> list[dict]:
    """
    Fetch multiple products concurrently using asyncio.

    Args:
        products: List of (product_id, url) tuples
        max_workers: Maximum concurrent requests
        timeout: Request timeout per product

    Returns:
        list[dict]: List of results from fetch_product_httpx()

    Note:
        This is a synchronous wrapper around httpx.Client.
        For true async, use httpx.AsyncClient with asyncio.
    """
    import concurrent.futures

    results = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(fetch_product_httpx, product_id, url, timeout): (
                product_id,
                url,
            )
            for product_id, url in products
        }

        for future in concurrent.futures.as_completed(futures):
            product_id, url = futures[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                logger.error(f"Error processing product {product_id}: {e}")
                results.append(
                    {
                        "product_id": product_id,
                        "url": url,
                        "status": "error",
                        "error_message": str(e),
                    }
                )

    return results


def test_mode(count: int = 20):
    """
    Test mode: Fetch N products from product_url_map.csv

    Args:
        count: Number of products to test (default: 20)
    """
    import csv
    import time
    import random

    # Load URLs from CSV
    input_file = project_root / "data" / "exports" / "product_url_map.csv"

    if not input_file.exists():
        logger.error(f"Input file not found: {input_file}")
        return 1

    logger.info(f"Loading {count} URLs from: {input_file}")

    products = []
    with open(input_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if i >= count:
                break
            products.append((row["product_id"], row["url"]))

    logger.info(f"Testing {len(products)} products (sequential, 1-3s random delay)")
    logger.info("=" * 70)

    # Process sequentially
    start_time = time.time()
    results = []

    for i, (product_id, url) in enumerate(products, 1):
        # Progress every 10 products
        if i % 10 == 0 or i == 1:
            elapsed = time.time() - start_time
            rate = i / elapsed if elapsed > 0 else 0
            eta = (len(products) - i) / rate if rate > 0 else 0
            logger.info(
                f"\n[{i}/{len(products)}] "
                f"Elapsed: {elapsed/60:.1f}min | "
                f"Rate: {rate:.2f} prod/s | "
                f"ETA: {eta/60:.1f}min"
            )

        result = fetch_product_httpx(product_id, url, timeout=30)
        results.append(result)

        if result["status"] == "success":
            if i <= 5 or i % 20 == 0:
                logger.info(f"  SUCCESS: {result.get('product_name', 'N/A')}")
        else:
            logger.warning(
                f"  FAILED - Product {product_id}: "
                f"{result['status']} - {result.get('error_message', 'N/A')}"
            )

        # Random delay to avoid pattern detection (human-like behavior)
        if i < len(products):
            delay = random.uniform(1.0, 3.0)
            time.sleep(delay)

    # Summary
    total_time = time.time() - start_time
    success_count = sum(1 for r in results if r["status"] == "success")
    http_403 = sum(1 for r in results if r.get("http_status") == 403)

    logger.info("\n" + "=" * 70)
    logger.info("TEST SUMMARY")
    logger.info("=" * 70)
    logger.info(f"Total tested: {len(results)}")
    logger.info(f"Success: {success_count} ({success_count/len(results)*100:.1f}%)")
    logger.info(f"HTTP 403: {http_403}")
    logger.info(f"Total time: {total_time/60:.2f} minutes")
    logger.info(f"Average rate: {len(results)/total_time:.2f} products/second")

    # Projected full crawl time
    total_products = 19417
    projected_time = total_products * (total_time / len(results))
    logger.info(f"\nProjected time for {total_products} products:")
    logger.info(f"  ~{projected_time/3600:.1f} hours")
    logger.info("=" * 70)

    # Save results
    output_file = project_root / "data" / "exports" / f"test_{count}_results.json"
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


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Fetch Glamira products using httpx")
    parser.add_argument(
        "--test",
        type=int,
        metavar="N",
        help="Test mode: fetch N products from CSV (default: 20)",
    )
    parser.add_argument("--url", type=str, help="Single URL to test")
    parser.add_argument(
        "--product-id", type=str, help="Product ID (required with --url)"
    )

    args = parser.parse_args()

    if args.test is not None:
        # Test mode
        count = args.test if args.test > 0 else 20
        sys.exit(test_mode(count))
    elif args.url and args.product_id:
        # Single URL test
        logger.info("Testing single product:")
        logger.info(f"  Product ID: {args.product_id}")
        logger.info(f"  URL: {args.url}\n")

        result = fetch_product_httpx(args.product_id, args.url)

        print("\nResult:")
        print(json.dumps(result, indent=2, ensure_ascii=False))

        sys.exit(0 if result["status"] == "success" else 1)
    else:
        # Default: quick test
        test_product_id = "99021"
        test_url = (
            "https://www.glamira.pt/"
            "glamira-ring-amazing-trust-5-mm.html"
            "?alloy=white-585"
        )

        logger.info("Quick test (use --test N or --url for more options)")
        logger.info(f"Product ID: {test_product_id}")
        logger.info(f"URL: {test_url}")

        result = fetch_product_httpx(test_product_id, test_url)

        print("\nResult:")
        print(json.dumps(result, indent=2, ensure_ascii=False))

        sys.exit(0 if result["status"] == "success" else 1)
