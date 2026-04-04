"""
Unit tests for ingestion.shared.gcs_writer module
"""

import gzip
import json
from pathlib import Path
from unittest.mock import patch

from ingestion.shared.gcs_writer import write_and_upload_jsonl_gz


class TestWriteAndUploadJsonlGz:
    """Test write_and_upload_jsonl_gz function"""

    @patch('ingestion.shared.gcs_writer.upload_to_gcs')
    def test_basic_success(self, mock_upload):
        """Test basic write, compress, upload workflow"""
        mock_upload.return_value = True

        test_records = [
            {'id': 1, 'name': 'test1', 'value': 100},
            {'id': 2, 'name': 'test2', 'value': 200},
            {'id': 3, 'name': 'test3', 'value': 300}
        ]

        stats = write_and_upload_jsonl_gz(
            records=iter(test_records),
            gcs_bucket='test_bucket',
            gcs_path='test/path.jsonl.gz',
            cleanup=False
        )

        # Verify upload was called
        mock_upload.assert_called_once()

        # Verify temp file exists and has correct JSONL.gz format
        temp_path = Path(stats['temp_file'])
        assert temp_path.exists()

        with gzip.open(temp_path, 'rt', encoding='utf-8') as f:
            lines = f.readlines()

        assert len(lines) == 3
        for idx, line in enumerate(lines):
            data = json.loads(line.strip())
            assert data == test_records[idx]

        # Cleanup
        temp_path.unlink()

    @patch('ingestion.shared.gcs_writer.upload_to_gcs')
    def test_return_stats_structure(self, mock_upload):
        """Test stats dictionary has correct structure and values"""
        mock_upload.return_value = True

        test_records = [
            {'id': 1, 'data': 'test1'},
            {'id': 2, 'data': 'test2'}
        ]

        stats = write_and_upload_jsonl_gz(
            records=iter(test_records),
            gcs_bucket='bucket',
            gcs_path='path.jsonl.gz',
            cleanup=True
        )

        # Check all required keys exist
        assert 'records' in stats
        assert 'uncompressed_bytes' in stats
        assert 'compressed_bytes' in stats
        assert 'compression_ratio' in stats
        assert 'gcs_uri' in stats
        assert 'success' in stats

        # Check values
        assert stats['records'] == 2
        assert stats['success'] is True
        assert stats['gcs_uri'] == 'gs://bucket/path.jsonl.gz'
        assert stats['uncompressed_bytes'] > 0
        assert stats['compressed_bytes'] > 0
        assert isinstance(stats['compression_ratio'], (int, float))

        # temp_file should not exist when cleanup=True
        assert 'temp_file' not in stats
