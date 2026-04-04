"""
Shared GCS Writer

Provides DRY utilities for writing data to GCS in standard formats.
"""

import gzip
import json
import tempfile
from pathlib import Path
from typing import Iterator, Dict, Any

from common.storage.gcs_client import upload_to_gcs
from common.utils.logger import get_logger

logger = get_logger(__name__)


def write_and_upload_jsonl_gz(
    records: Iterator[Dict[str, Any]],
    gcs_bucket: str,
    gcs_path: str,
    cleanup: bool = True
) -> Dict[str, Any]:
    """
    Write records to temporary JSONL.gz file, upload to GCS, and return stats.

    Args:
        records: Iterator of JSON-serializable dictionaries
        gcs_bucket: GCS bucket name (e.g., "raw_glamira")
        gcs_path: Destination path in GCS (e.g., "raw/events/events_20260404.jsonl.gz")
        cleanup: Whether to delete temp file after upload (default: True)

    Returns:
        dict: Statistics with keys:
            - records: int - Number of records processed
            - uncompressed_bytes: int - Size before compression
            - compressed_bytes: int - Size after compression
            - compression_ratio: float - Compression ratio (0-100%)
            - gcs_uri: str - Full GCS URI (gs://bucket/path)
            - success: bool - Upload success status
            - temp_file: str - Temp file path (if cleanup=False)

    Raises:
        Exception: If upload fails or iteration error occurs
    """
    # Create temp file
    temp_file = tempfile.NamedTemporaryFile(
        mode='wb',
        suffix='.jsonl.gz',
        delete=False
    )
    temp_path = Path(temp_file.name)

    logger.info(f"Writing JSONL.gz to temp file: {temp_path}")

    record_count = 0
    uncompressed_size = 0

    try:
        # Write compressed JSONL
        with gzip.open(temp_path, 'wt', encoding='utf-8') as gz_file:
            for record in records:
                json_line = json.dumps(record, ensure_ascii=False)
                gz_file.write(json_line + '\n')

                record_count += 1
                uncompressed_size += len(json_line.encode('utf-8')) + 1

                if record_count % 100000 == 0:
                    logger.info(f"Processed {record_count:,} records...")

        compressed_size = temp_path.stat().st_size
        compression_ratio = (
            (1 - compressed_size / uncompressed_size) * 100
            if uncompressed_size > 0 else 0
        )

        logger.info(
            f"Compression complete: {record_count:,} records, "
            f"{uncompressed_size / (1024**2):.2f} MB -> "
            f"{compressed_size / (1024**2):.2f} MB "
            f"(ratio: {compression_ratio:.1f}%)"
        )

        # Upload to GCS using common client
        gcs_uri = f"gs://{gcs_bucket}/{gcs_path}"
        logger.info(f"Uploading to {gcs_uri}")

        success = upload_to_gcs(
            local_file_path=temp_path,
            bucket_name=gcs_bucket,
            destination_blob_name=gcs_path,
            overwrite=True
        )

        if success:
            logger.info(f"Upload successful: {gcs_uri}")
        else:
            logger.error("Upload failed")

        # Prepare stats
        stats = {
            "records": record_count,
            "uncompressed_bytes": uncompressed_size,
            "compressed_bytes": compressed_size,
            "compression_ratio": round(compression_ratio, 2),
            "gcs_uri": gcs_uri,
            "success": success
        }

        if not cleanup:
            stats["temp_file"] = str(temp_path)

        return stats

    finally:
        # Cleanup temp file
        if cleanup and temp_path.exists():
            temp_path.unlink()
            logger.info(f"Cleaned up temp file: {temp_path}")
        elif not cleanup:
            logger.info(f"Temp file retained: {temp_path}")
