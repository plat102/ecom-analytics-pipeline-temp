"""
Unit tests for utils.py - Utility functions.

These tests validate helper functions without external dependencies.
"""

from ingestion.product_crawler.utils import (
    clean_url,
    summarize_results,
    get_browser_headers,
)


class TestCleanURL:
    """Test URL cleaning - removes tracking params."""

    def test_clean_url_removes_fbclid_parameter(self):
        """Should remove Facebook tracking parameter."""
        url = "https://glamira.com/ring.html?fbclid=IwAR123&alloy=gold"

        result = clean_url(url)

        assert "fbclid" not in result
        assert "alloy=gold" in result

    def test_clean_url_removes_utm_parameters(self):
        """Should remove all UTM tracking parameters."""
        url = "https://glamira.com/ring.html?utm_source=fb&utm_campaign=spring&stone=diamond"

        result = clean_url(url)

        assert "utm_source" not in result
        assert "utm_campaign" not in result
        assert "stone=diamond" in result

    def test_clean_url_unchanged_when_no_params(self):
        """Should return URL unchanged if no query params."""
        url = "https://glamira.com/ring.html"

        result = clean_url(url)

        assert result == url


class TestSummarizeResults:
    """Test statistics calculation for crawl results."""

    def test_summarize_results_with_all_success(self):
        """Should calculate 100% success rate."""
        results = [
            {"product_id": "1", "status": "success"},
            {"product_id": "2", "status": "success"},
            {"product_id": "3", "status": "success"},
        ]

        summary = summarize_results(results)

        assert summary["total"] == 3
        assert summary["success"] == 3
        assert summary["success_rate"] == 100.0
        assert summary["errors"] == 0

    def test_summarize_results_with_mixed_status(self):
        """Should calculate correct stats for mixed results."""
        results = [
            {"product_id": "1", "status": "success"},
            {"product_id": "2", "status": "error", "http_status": 403},
            {"product_id": "3", "status": "success"},
            {"product_id": "4", "status": "no_react_data"},
        ]

        summary = summarize_results(results)

        assert summary["total"] == 4
        assert summary["success"] == 2
        assert summary["success_rate"] == 50.0
        assert summary["errors"] == 1
        assert summary["no_react_data"] == 1
        assert summary["http_403"] == 1

    def test_summarize_results_with_empty_list(self):
        """Should handle empty results gracefully."""
        results = []

        summary = summarize_results(results)

        assert summary["total"] == 0
        assert summary["success"] == 0
        assert summary["success_rate"] == 0.0


class TestGetBrowserHeaders:
    """Test browser headers generation."""

    def test_get_browser_headers_returns_dict(self):
        """Should return dictionary with HTTP headers."""
        headers = get_browser_headers()

        assert isinstance(headers, dict)
        assert "Accept" in headers
        assert "Accept-Language" in headers
        assert "User-Agent" not in headers  # User-Agent set separately
