"""Unit tests for HTTP caching functionality."""

import pytest

from oda_reader import (
    clear_http_cache,
    common,
    disable_http_cache,
    enable_http_cache,
    get_http_cache_info,
)


@pytest.mark.unit
@pytest.mark.cache
@pytest.mark.xdist_group("cache")
class TestHTTPCache:
    """Test HTTP cache control functions.

    Note: These tests are grouped to run serially (not in parallel)
    because they test global cache state that can't be safely shared
    across pytest-xdist workers.
    """

    def test_disable_cache_sets_flag(self):
        """Test that disable_http_cache sets the flag."""
        enable_http_cache()
        assert common._CACHE_ENABLED is True

        disable_http_cache()

        # Check the global variable through the module
        assert common._CACHE_ENABLED is False

        # Cleanup
        enable_http_cache()

    def test_enable_cache_sets_flag(self):
        """Test that enable_http_cache sets the flag."""
        disable_http_cache()

        enable_http_cache()

        assert common._CACHE_ENABLED is True

    def test_clear_cache_resets_counters(self):
        """Test that clear_http_cache resets cache statistics."""
        enable_http_cache()
        clear_http_cache()

        info = get_http_cache_info()

        assert info["response_count"] == 0
        assert info["redirects_count"] == 0

    def test_get_cache_info_returns_dict(self):
        """Test that get_http_cache_info returns expected structure."""
        enable_http_cache()

        info = get_http_cache_info()

        assert isinstance(info, dict)
        assert "response_count" in info
        assert "redirects_count" in info
