"""
Parallel web scraper for Glamira product names using Playwright.

Features:
- Multi-worker crawling with ThreadPoolExecutor
- Per-worker checkpoint and output files (no race conditions)
- Each worker = 1 browser instance
- Merge results after completion

Usage:
    python crawl_products_parallel.py --workers 4
"""

import argparse
import csv
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from playwright.sync_api import sync_playwright

# Add project root to sys.path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from common.utils.logger import get_logger
from ingestion.product_crawler.utils import (
    fetch_html_with_playwright,
    parse_product_name,
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
WORKER_OUTPUT_DIR = DATA_DIR / "workers"

# Rate limiting (faster for parallel)
SLEEP_MIN = 0.2
SLEEP_MAX = 0.5


def crawl_worker(worker_id: int, products: list, output_dir: Path):
    """
    Worker function - crawls assigned products.

    Args:
        worker_id: Worker ID (0-based)
        products: List of (product_id, url) tuples
        output_dir: Directory for worker outputs

    Returns:
        dict: Worker stats
    """
    # Worker files
    output_file = output_dir / f"worker_{worker_id}_names.csv"
    checkpoint_file = output_dir / f"worker_{worker_id}_checkpoint.json"
    error_file = output_dir / f"worker_{worker_id}_errors.jsonl"

    # Load checkpoint
    processed_ids, processed_count = load_checkpoint(checkpoint_file)

    # Filter unprocessed
    products_to_process = [(pid, url) for pid, url in products if pid not in processed_ids]

    logger.info(f"Worker {worker_id}: {len(products_to_process)} products to process")

    if len(products_to_process) == 0:
        return {"worker_id": worker_id, "success": 0, "errors": 0, "total": 0}

    success_count = 0
    error_count = 0

    # Start Playwright browser
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        logger.info(f"Worker {worker_id}: Browser launched")

        # Prepare output file
        file_exists = output_file.exists()
        mode = 'a' if file_exists else 'w'

        with open(output_file, mode, encoding='utf-8', newline='') as f:
            writer = csv.writer(f)

            # Write header if new file
            if not file_exists:
                writer.writerow(['product_id', 'product_name', 'url'])

            for product_id, url in products_to_process:
                # Fetch HTML
                html = fetch_html_with_playwright(page, url, product_id, error_file)

                if html is None:
                    error_count += 1
                    writer.writerow([product_id, '', url])
                    f.flush()
                else:
                    # Parse product name
                    product_name = parse_product_name(html)

                    if product_name is None:
                        log_error(error_file, product_id, url, "ParseError", message="No product name found")
                        error_count += 1

                    writer.writerow([product_id, product_name or '', url])
                    f.flush()

                    if product_name:
                        success_count += 1

                # Update checkpoint
                processed_ids.add(product_id)
                processed_count += 1

                if processed_count % CHECKPOINT_INTERVAL == 0:
                    save_checkpoint(checkpoint_file, processed_ids, processed_count, mode="parallel")
                    logger.info(f"Worker {worker_id}: Progress {processed_count}/{len(products)} | Success: {success_count} | Errors: {error_count}")

                # Rate limiting
                time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))

        browser.close()

    # Final checkpoint
    save_checkpoint(checkpoint_file, processed_ids, processed_count, mode="parallel")

    logger.info(f"Worker {worker_id}: Complete - Success: {success_count}, Errors: {error_count}")

    return {
        "worker_id": worker_id,
        "success": success_count,
        "errors": error_count,
        "total": len(products_to_process)
    }


def merge_worker_outputs(output_dir: Path, final_output: Path, num_workers: int):
    """Merge all worker CSV files into final output."""
    logger.info("Merging worker outputs...")

    final_output.parent.mkdir(parents=True, exist_ok=True)

    with open(final_output, 'w', encoding='utf-8', newline='') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(['product_id', 'product_name', 'url'])

        for worker_id in range(num_workers):
            worker_file = output_dir / f"worker_{worker_id}_names.csv"

            if not worker_file.exists():
                logger.warning(f"Worker {worker_id} output not found: {worker_file}")
                continue

            with open(worker_file, 'r', encoding='utf-8') as infile:
                reader = csv.reader(infile)
                next(reader)  # Skip header

                for row in reader:
                    writer.writerow(row)

    logger.info(f" Merged output saved to: {final_output}")


def main():
    parser = argparse.ArgumentParser(description="Parallel Glamira product name crawler")
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
        help='Final merged output CSV file'
    )
    parser.add_argument(
        '--workers',
        type=int,
        default=4,
        help='Number of parallel workers (default: 4)'
    )

    args = parser.parse_args()

    if not args.input.exists():
        logger.error(f"Input file not found: {args.input}")
        sys.exit(1)

    # Load products
    logger.info(f"Loading products from {args.input}")
    products = []
    with open(args.input, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            products.append((row['product_id'], row['url']))

    logger.info(f"Loaded {len(products)} products")

    # Split products into chunks for workers
    chunk_size = len(products) // args.workers
    remainder = len(products) % args.workers

    product_chunks = []
    start = 0

    for i in range(args.workers):
        # Distribute remainder across first workers
        size = chunk_size + (1 if i < remainder else 0)
        end = start + size
        product_chunks.append(products[start:end])
        start = end

    # Create worker output directory
    WORKER_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Start parallel crawling
    logger.info(f"Starting {args.workers} workers...")
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(crawl_worker, i, chunk, WORKER_OUTPUT_DIR): i
            for i, chunk in enumerate(product_chunks)
        }

        # Collect results
        total_success = 0
        total_errors = 0

        for future in as_completed(futures):
            worker_id = futures[future]
            try:
                stats = future.result()
                total_success += stats['success']
                total_errors += stats['errors']
            except Exception as e:
                logger.error(f"Worker {worker_id} failed: {e}")

    # Merge outputs
    merge_worker_outputs(WORKER_OUTPUT_DIR, args.output, args.workers)

    # Summary
    elapsed = time.time() - start_time
    logger.info("=" * 60)
    logger.info("Parallel Crawl Complete")
    logger.info(f"Workers: {args.workers}")
    logger.info(f"Total products: {len(products)}")
    logger.info(f"Success: {total_success}")
    logger.info(f"Errors: {total_errors}")
    logger.info(f"Success rate: {total_success / len(products) * 100:.2f}%")
    logger.info(f"Time elapsed: {elapsed / 60:.2f} minutes")
    logger.info(f"Speed: {len(products) / elapsed:.2f} products/second")
    logger.info(f"Output: {args.output}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
