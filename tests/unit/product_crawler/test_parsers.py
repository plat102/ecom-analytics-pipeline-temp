"""
Unit tests for parsers.py - HTML parsing functions.

These tests validate the core parsing logic without any external dependencies.
"""

import pytest
from ingestion.sources.products.parsers import extract_react_data


class TestExtractReactData:
    """Test react_data extraction from HTML."""

    def test_extract_react_data_with_valid_json(self):
        """Should extract valid JSON from react_data variable."""
        html = '''
        <html>
        <head><title>Product Page</title></head>
        <body>
            <script>
            var react_data = {"product_id": 123, "name": "Gold Ring", "price": 99.99};
            </script>
        </body>
        </html>
        '''

        result = extract_react_data(html)

        assert result is not None
        assert result["product_id"] == 123
        assert result["name"] == "Gold Ring"
        assert result["price"] == 99.99

    def test_extract_react_data_returns_none_when_missing(self):
        """Should return None if react_data not found in HTML."""
        html = '''
        <html>
        <head><title>Category Page</title></head>
        <body>
            <h1>Product Listing</h1>
            <p>No react_data here</p>
        </body>
        </html>
        '''

        result = extract_react_data(html)

        assert result is None

    def test_extract_react_data_returns_none_when_invalid_json(self):
        """Should return None if JSON is malformed."""
        html = '''
        <html>
        <body>
            <script>
            var react_data = {invalid json, missing quotes};
            </script>
        </body>
        </html>
        '''

        result = extract_react_data(html)

        assert result is None
