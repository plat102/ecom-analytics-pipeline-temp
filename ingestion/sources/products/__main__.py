"""
Product Crawler Module - CLI Entry Point.

Unified command-line interface for the complete product crawling pipeline:
1. Extract URLs from MongoDB
2. Crawl products (test or full mode)
3. Retry failed products (DLQ)
4. Upload results to GCS

Usage:
    # Step 1: Extract product URLs from MongoDB
    python -m ingestion.sources.products extract [--output FILE]

    # Step 2: Crawl products
    python -m ingestion.sources.products crawl [--test N] [--resume] [--concurrency N]

    # Step 3: Retry failed products
    python -m ingestion.sources.products retry [--403-only] [--analyze]

    # Step 4: Upload to GCS
    python -m ingestion.sources.products upload --file results.json

    # Run full pipeline
    python -m ingestion.sources.products pipeline [--upload]

Note: retry module can also run standalone:
    python -m ingestion.sources.products.retry
"""

import sys
import argparse
import asyncio
from pathlib import Path
from datetime import datetime

from common.utils.logger import get_logger

logger = get_logger(__name__)


def generate_retry_filename(prefix: str = "retry") -> Path:
    """
    Generate timestamped filename for retry output.

    Args:
        prefix: Filename prefix (e.g., "retry" or "retry_403")

    Returns:
        Path: Full path with timestamp (e.g., data/exports/retry_20260329_143022.json)
    """
    from ingestion.sources.products import config
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{prefix}_{timestamp}.json"
    return config.OUTPUT_DIR / filename


def create_parser() -> argparse.ArgumentParser:
    """Create main argument parser with subcommands."""
    parser = argparse.ArgumentParser(
        prog="python -m ingestion.sources.products",
        description="Product Crawler Pipeline - Extract, Crawl, Retry",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Extract URLs from MongoDB
  python -m ingestion.sources.products extract

  # Test crawl 50 products
  python -m ingestion.sources.products crawl --test 50

  # Full crawl with resume
  python -m ingestion.sources.products crawl --resume

  # Retry failed products
  python -m ingestion.sources.products retry
  python -m ingestion.sources.products retry --403-only
  python -m ingestion.sources.products retry --analyze

  # Upload results to GCS
  python -m ingestion.sources.products upload --file data/exports/full_crawl_results.json

  # Run complete pipeline
  python -m ingestion.sources.products pipeline --upload
        """
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # ========================================================================
    # EXTRACT command
    # ========================================================================
    extract_parser = subparsers.add_parser(
        "extract",
        help="Extract product URLs from MongoDB"
    )
    extract_parser.add_argument(
        "--output",
        type=str,
        help="Output CSV file path (default: data/exports/product_url_map.csv)"
    )

    # ========================================================================
    # CRAWL command
    # ========================================================================
    crawl_parser = subparsers.add_parser(
        "crawl",
        help="Crawl products (test or full mode)"
    )
    crawl_parser.add_argument(
        "--test",
        type=int,
        metavar="N",
        help="Test mode: crawl N products (default: full crawl)"
    )
    crawl_parser.add_argument(
        "--concurrency",
        type=int,
        help="Max concurrent requests (default: 20 for test, 15 for full)"
    )
    crawl_parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from checkpoint (skip already processed products)"
    )
    crawl_parser.add_argument(
        "--no-checkpoint",
        action="store_true",
        help="Disable checkpointing"
    )
    crawl_parser.add_argument(
        "--output",
        type=str,
        help="Custom output JSON file path"
    )

    # ========================================================================
    # RETRY command
    # ========================================================================
    retry_parser = subparsers.add_parser(
        "retry",
        help="Retry failed products (Dead Letter Queue)"
    )
    retry_parser.add_argument(
        "--403-only",
        action="store_true",
        dest="only_403",
        help="Retry only 403 errors with curl_cffi (TLS spoofing)"
    )
    retry_parser.add_argument(
        "--analyze",
        action="store_true",
        help="Analyze failure patterns (no retry)"
    )
    retry_parser.add_argument(
        "--input",
        type=str,
        help="Input JSON file path (default: data/exports/full_crawl_results.json)"
    )
    retry_parser.add_argument(
        "--output",
        type=str,
        help="Output JSON file path (default: auto-generated)"
    )

    # ========================================================================
    # PIPELINE command (run all steps)
    # ========================================================================
    pipeline_parser = subparsers.add_parser(
        "pipeline",
        help="Run complete pipeline: extract -> crawl -> retry -> upload"
    )
    pipeline_parser.add_argument(
        "--test",
        type=int,
        metavar="N",
        help="Test mode: crawl only N products (default: full pipeline)"
    )
    pipeline_parser.add_argument(
        "--skip-extract",
        action="store_true",
        help="Skip extraction (use existing CSV)"
    )
    pipeline_parser.add_argument(
        "--skip-retry",
        action="store_true",
        help="Skip retry step"
    )
    pipeline_parser.add_argument(
        "--upload",
        action="store_true",
        help="Upload results to GCS after completion"
    )

    # ========================================================================
    # UPLOAD command (upload existing files to GCS)
    # ========================================================================
    upload_parser = subparsers.add_parser(
        "upload",
        help="Upload results to Google Cloud Storage"
    )
    upload_parser.add_argument(
        "--file",
        type=str,
        required=True,
        help="File to upload (e.g., data/exports/full_crawl_results.json)"
    )
    upload_parser.add_argument(
        "--bucket",
        type=str,
        help="GCS bucket name (default: from config)"
    )
    upload_parser.add_argument(
        "--destination",
        type=str,
        help="Destination path in GCS (default: auto-generated from filename)"
    )

    return parser


async def run_pipeline(args):
    """Run complete pipeline: extract -> crawl -> retry -> upload."""
    from ingestion.sources.products import config

    logger.info("=" * 70)
    logger.info("RUNNING COMPLETE PIPELINE")
    logger.info("=" * 70)

    final_output_file = None

    # Step 1: Extract URLs (optional)
    if not args.skip_extract:
        logger.info("\n>>> STEP 1: EXTRACT PRODUCT URLS <<<")
        from ingestion.sources.products.extractor import extract_product_urls
        count = extract_product_urls()
        logger.info(f"Extracted {count} product URLs")
    else:
        logger.info("\n>>> STEP 1: SKIPPED (using existing CSV) <<<")

    # Step 2: Crawl
    logger.info("\n>>> STEP 2: CRAWL PRODUCTS <<<")
    from ingestion.sources.products.crawler import run_crawl

    exit_code = await run_crawl(
        limit=args.test,
        concurrency=None,
        resume=False,
        checkpoint=True
    )

    if exit_code != 0:
        logger.warning(f"Crawl completed with warnings (exit code: {exit_code})")

    # Determine output file for upload (use constants from config)
    final_output_file = config.FULL_CRAWL_OUTPUT

    # Step 3: Retry (optional)
    if not args.skip_retry:
        logger.info("\n>>> STEP 3: RETRY FAILED PRODUCTS <<<")
        from ingestion.sources.products.retry import retry_failed_products

        # Generate timestamped retry output
        retry_output = generate_retry_filename("retry")

        merged_results = await retry_failed_products(
            input_file=config.FULL_CRAWL_OUTPUT,
            output_file=retry_output
        )

        if merged_results:
            logger.info(f"Final results: {retry_output}")
            final_output_file = retry_output  # Use merged file for upload
    else:
        logger.info("\n>>> STEP 3: SKIPPED (no retry) <<<")

    # Step 4: Upload to GCS (optional)
    if args.upload:
        logger.info("\n>>> STEP 4: UPLOAD TO GCS <<<")
        upload_result_file(final_output_file)
    else:
        logger.info("\n>>> STEP 4: SKIPPED (no upload) <<<")

    logger.info("\n" + "=" * 70)
    logger.info("PIPELINE COMPLETE!")
    logger.info("=" * 70)


def upload_result_file(file_path: Path) -> bool:
    """
    Upload a result file to GCS.

    Args:
        file_path: Path to file to upload

    Returns:
        bool: True if successful, False otherwise
    """
    from datetime import date
    from ingestion.sources.products import config
    from common.storage import upload_to_gcs

    if not file_path.exists():
        logger.error(f"File not found: {file_path}")
        return False

    # Generate destination blob name with date
    # Format: raw/glamira/products/YYYYMMDD/filename.json
    today = date.today().strftime("%Y%m%d")
    destination = f"{config.GCS_DESTINATION_PREFIX}/{today}/{file_path.name}"

    logger.info(f"Uploading {file_path.name} to GCS...")
    logger.info(f"  Bucket: {config.GCS_BUCKET_NAME}")
    logger.info(f"  Destination: {destination}")

    success = upload_to_gcs(
        local_file_path=file_path,
        bucket_name=config.GCS_BUCKET_NAME,
        destination_blob_name=destination,
        overwrite=True
    )

    if success:
        logger.info(f"Upload successful: gs://{config.GCS_BUCKET_NAME}/{destination}")
    else:
        logger.error("Upload failed")

    return success


def main():
    """Main entry point."""
    parser = create_parser()
    args = parser.parse_args()

    # Show help if no command specified
    if not args.command:
        parser.print_help()
        sys.exit(1)

    # Route to appropriate handler
    if args.command == "extract":
        from ingestion.sources.products.extractor import extract_product_urls

        output_file = Path(args.output) if args.output else None
        extract_product_urls(output_file)

    elif args.command == "crawl":
        from ingestion.sources.products.crawler import run_crawl

        output_file = Path(args.output) if args.output else None

        exit_code = asyncio.run(run_crawl(
            output_file=output_file,
            concurrency=args.concurrency,
            limit=args.test,
            resume=args.resume,
            checkpoint=not args.no_checkpoint
        ))

        sys.exit(exit_code)

    elif args.command == "retry":
        from ingestion.sources.products.retry import (
            retry_failed_products,
            retry_403_with_curlcffi,
            analyze_failures
        )
        from ingestion.sources.products import config

        # Parse input path (default: original crawl results)
        input_file = Path(args.input) if args.input else config.FULL_CRAWL_OUTPUT

        # Handle analyze mode
        if args.analyze:
            logger.info("FAILURE ANALYSIS MODE")
            logger.info("=" * 70)
            analyze_failures(input_file)
            sys.exit(0)

        # Handle 403-only mode with curl_cffi
        if args.only_403:
            logger.info("Retry 403 errors with curl_cffi (TLS spoofing)")
            logger.info("=" * 70)

            # Generate timestamped output
            output_file = Path(args.output) if args.output else generate_retry_filename("retry_403")

            logger.info(f"Input: {input_file}")
            logger.info(f"Output: {output_file}")
            logger.info("=" * 70)

            merged_results = retry_403_with_curlcffi(input_file, output_file)

            if not merged_results:
                logger.info("No 403 errors to retry. Exiting.")
                sys.exit(0)

            logger.info("\nRetry complete!")
            logger.info(f"Merged results saved to: {output_file}")
            sys.exit(0)

        # Normal retry with httpx
        logger.info("Dead Letter Queue (DLQ) Retry for Failed Products")
        logger.info("=" * 70)

        # Generate timestamped output
        output_file = Path(args.output) if args.output else generate_retry_filename("retry")

        logger.info(f"Input: {input_file}")
        logger.info(f"Output: {output_file}")
        logger.info("=" * 70)

        merged_results = asyncio.run(retry_failed_products(
            input_file=input_file,
            output_file=output_file
        ))

        if not merged_results:
            logger.info("No products to retry. Exiting.")
            sys.exit(0)

        logger.info("\nDLQ Retry complete!")
        logger.info(f"Merged results saved to: {output_file}")

    elif args.command == "pipeline":
        asyncio.run(run_pipeline(args))

    elif args.command == "upload":
        from datetime import date
        from ingestion.sources.products import config
        from common.storage import upload_to_gcs

        # Parse file path
        file_path = Path(args.file)

        if not file_path.exists():
            logger.error(f"File not found: {file_path}")
            sys.exit(1)

        # Determine bucket
        bucket_name = args.bucket if args.bucket else config.GCS_BUCKET_NAME

        # Determine destination
        if args.destination:
            destination = args.destination
        else:
            # Auto-generate: raw/glamira/products/YYYYMMDD/filename.json
            today = date.today().strftime("%Y%m%d")
            destination = f"{config.GCS_DESTINATION_PREFIX}/{today}/{file_path.name}"

        logger.info(f"Uploading: {file_path}")
        logger.info(f"  -> gs://{bucket_name}/{destination}")

        success = upload_to_gcs(
            local_file_path=file_path,
            bucket_name=bucket_name,
            destination_blob_name=destination,
            overwrite=True
        )

        if success:
            logger.info("Upload successful")
            sys.exit(0)
        else:
            logger.error("Upload failed")
            sys.exit(1)

    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
