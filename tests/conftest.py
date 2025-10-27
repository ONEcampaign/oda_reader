"""Shared pytest fixtures and configuration."""

from pathlib import Path

import pytest

from oda_reader import disable_http_cache, enable_http_cache
from oda_reader.common import RateLimiter


@pytest.fixture(autouse=True)
def disable_cache_for_tests():
    """Disable HTTP cache for all tests by default."""
    disable_http_cache()
    yield
    enable_http_cache()


@pytest.fixture
def temp_cache_dir(tmp_path, monkeypatch):
    """Create and configure a temporary cache directory for testing.

    Args:
        tmp_path: pytest's temporary directory fixture
        monkeypatch: pytest's monkeypatch fixture

    Yields:
        Path: Path to the temporary cache directory
    """
    cache_dir = tmp_path / "test_cache"
    cache_dir.mkdir()
    monkeypatch.setenv("ODA_READER_CACHE_DIR", str(cache_dir))
    yield cache_dir


@pytest.fixture
def rate_limiter_fast():
    """Create a fast rate limiter for testing (2 calls per 0.5 seconds).

    Returns:
        RateLimiter: Configured rate limiter for testing
    """
    return RateLimiter(max_calls=2, period=0.5)


@pytest.fixture
def sample_csv_response():
    """Return a sample CSV response string for mocking API responses.

    Returns:
        str: Sample CSV data as string
    """
    return """DONOR,RECIPIENT,TIME_PERIOD,OBS_VALUE
1,503,2023,1000.5
1,503,2024,1500.75
"""


@pytest.fixture
def fixtures_dir():
    """Return the path to the fixtures directory.

    Returns:
        Path: Path to tests/fixtures/
    """
    return Path(__file__).parent / "fixtures"
