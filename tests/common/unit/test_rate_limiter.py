"""Unit tests for RateLimiter class."""

import time

import pytest

from oda_reader.common import RateLimiter


@pytest.mark.unit
class TestRateLimiterBlocking:
    """Test rate limiter blocking behavior."""

    def test_blocks_when_limit_exceeded(self):
        """Verify rate limiter blocks when limit is reached."""
        limiter = RateLimiter(max_calls=2, period=1.0)

        start = time.time()
        for _ in range(3):
            limiter.wait()
        elapsed = time.time() - start

        # Third call should block for approximately 1 second
        assert elapsed >= 1.0, f"Expected blocking >= 1.0s, got {elapsed:.2f}s"
        assert elapsed < 1.2, f"Blocking took too long: {elapsed:.2f}s"

    def test_no_blocking_within_limit(self):
        """Verify rate limiter doesn't block when within limit."""
        limiter = RateLimiter(max_calls=5, period=1.0)

        start = time.time()
        for _ in range(3):
            limiter.wait()
        elapsed = time.time() - start

        # Should complete almost immediately
        assert elapsed < 0.1, f"Unexpected blocking: {elapsed:.2f}s"

    def test_configuration_changes_take_effect(self):
        """Verify changing rate limiter configuration works."""
        limiter = RateLimiter(max_calls=2, period=0.5)

        # Use up the initial limit
        limiter.wait()
        limiter.wait()

        # Change configuration
        limiter.max_calls = 10
        limiter.period = 0.5

        # Should not block now
        start = time.time()
        limiter.wait()
        elapsed = time.time() - start

        assert elapsed < 0.1, "Should not block after increasing limit"
