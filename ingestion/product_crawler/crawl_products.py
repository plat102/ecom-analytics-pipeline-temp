"""
Sequential web scraper to extract product names from Glamira website.

Features:
- Checkpoint system with set of processed IDs (resume capability)
- Error logging (JSONL format)
- Incremental CSV saves (crash resilience)
- Rate limiting with random sleep
- Retry with exponential backoff
"""

import argparse
import csv
import random
import sys
import time
from pathlib import Path

from playwright.sync_api import sync_playwright

# Add project root to sys.path for imports
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from common.utils.logger import get_logger
from ingestion.product_crawler.parsers import parse_product_name
from ingestion.product_crawler.utils import (
    fetch_html_with_playwright,
    log_error,
    save_checkpoint,
    load_checkpoint,
    CHECKPOINT_INTERVAL
)

logger = get_logger(__name__)

# Paths
DATA_DIR = project_root / "data" / "exports"
DEFAULT_INPUT = DATA_DIR / "product_url_map.csv"
DEFAULT_OUTPUT = DATA_DIR / "product_names.csv"
CHECKPOINT_FILE = DATA_DIR / "crawler_checkpoint.json"
ERROR_LOG = DATA_DIR / "crawler_errors.jsonl"

# Rate limiting
SLEEP_MIN = 0.5
SLEEP_MAX = 1.5


def crawl_products(input_file, output_file):
    """
    Main crawler loop.

    - Load checkpoint
    - Skip already processed products
    - Fetch HTML + parse product name
    - Incremental CSV save
    - Update checkpoint every 100 products
    """

    # Load checkpoint
    processed_ids, processed_count = load_checkpoint(CHECKPOINT_FILE)
    if processed_count > 0:
        logger.info(f"Resuming from checkpoint: {processed_count} products already processed")
    else:
        logger.info("No checkpoint found, starting fresh")

    # Load product URLs from CSV
    logger.info(f"Loading products from {input_file}")
    products = []
    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            products.append((row['product_id'], row['url']))

    logger.info(f"Loaded {len(products)} products from CSV")

    # Filter out already processed
    products_to_process = [(pid, url) for pid, url in products if pid not in processed_ids]
    logger.info(f"Resuming: skipping {len(processed_ids)} already processed products")
    logger.info(f"Products to process: {len(products_to_process)}")

    if len(products_to_process) == 0:
        logger.info("All products already processed")
        return

    # Prepare output file
    output_file.parent.mkdir(parents=True, exist_ok=True)

    # Check if output file exists to determine write mode
    file_exists = output_file.exists()
    mode = 'a' if file_exists else 'w'

    # Start crawling with Playwright
    logger.info("Starting crawler with Playwright...")
    success_count = 0
    error_count = 0
    start_time = time.time()

    with sync_playwright() as p:
        # Launch browser once (reuse for all requests)
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        logger.info("Playwright browser launched")

        with open(output_file, mode, encoding='utf-8', newline='') as f:
            writer = csv.writer(f)

            # Write header if new file
            if not file_exists:
                writer.writerow(['product_id', 'product_name', 'url'])

            for product_id, url in products_to_process:
                # Fetch HTML with Playwright
                html = fetch_html_with_playwright(page, url, product_id, ERROR_LOG)

                if html is None:
                    error_count += 1
                    # Still write row with empty name
                    writer.writerow([product_id, '', url])
                    f.flush()
                else:
                    # Parse product name
                    product_name = parse_product_name(html)

                    if product_name is None:
                        log_error(ERROR_LOG, product_id, url, "ParseError", message="No product name found")
                        error_count += 1

                    # Write to CSV
                    writer.writerow([product_id, product_name or '', url])
                    f.flush()  # Force write to disk

                    if product_name:
                        success_count += 1

                # Update checkpoint
                processed_ids.add(product_id)
                processed_count += 1

                if processed_count % CHECKPOINT_INTERVAL == 0:
                    save_checkpoint(CHECKPOINT_FILE, processed_ids, processed_count)
                    logger.info(f"Progress: {processed_count}/{len(products)} | Success: {success_count} | Errors: {error_count}")

                # Rate limiting
                time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))

        browser.close()
        logger.info("Playwright browser closed")

    # Final checkpoint save
    save_checkpoint(CHECKPOINT_FILE, processed_ids, processed_count)

    # Summary
    elapsed = time.time() - start_time
    logger.info("=" * 60)
    logger.info("Crawl Complete")
    logger.info(f"Total processed: {len(products_to_process)}")
    logger.info(f"Success: {success_count}")
    logger.info(f"Errors: {error_count}")
    logger.info(f"Success rate: {success_count / len(products_to_process) * 100:.2f}%")
    logger.info(f"Time elapsed: {elapsed / 60:.2f} minutes")
    logger.info(f"Output: {output_file}")
    logger.info("=" * 60)


def main():
    parser = argparse.ArgumentParser(description="Crawl Glamira product names")
    parser.add_argument(
        '--input',
        type=Path,
        default=DEFAULT_INPUT,
        help='Input CSV file with product_id and url'
    )
    parser.add_argument(
        '--output',
        type=Path,
        default=DEFAULT_OUTPUT,
        help='Output CSV file with product names'
    )

    args = parser.parse_args()

    if not args.input.exists():
        logger.error(f"Input file not found: {args.input}")
        sys.exit(1)

    crawl_products(args.input, args.output)


if __name__ == "__main__":
    main()
