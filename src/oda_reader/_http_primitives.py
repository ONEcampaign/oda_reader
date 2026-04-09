"""Shared HTTP primitives used by both common.py and version_discovery.py.

This module exists to break the circular import that would arise if
version_discovery.py imported from common.py while common.py imports
discover_latest_version from version_discovery.py.

This module must not import from common.py or version_discovery.py to avoid
circular imports.
"""

import logging
import time
from collections import deque

import requests
import requests_cache

from oda_reader._cache.config import get_http_cache_path

logger = logging.getLogger("oda_importer")

# ---------------------------------------------------------------------------
# Rate limiter
# ---------------------------------------------------------------------------


class RateLimiter:
    """Simple blocking rate limiter.

    Parameters correspond to the maximum number of calls allowed within
    ``period`` seconds. ``wait`` pauses execution when the limit has been
    reached.
    """

    def __init__(self, max_calls: int = 20, period: float = 60.0) -> None:
        self.max_calls = max_calls
        self.period = period
        self._calls: deque[float] = deque()

    def wait(self) -> None:
        """Block until a new call is allowed."""
        now = time.monotonic()
        while self._calls and now - self._calls[0] >= self.period:
            self._calls.popleft()
        if len(self._calls) >= self.max_calls:
            sleep_for = self.period - (now - self._calls[0])
            time.sleep(max(sleep_for, 0))
            self._calls.popleft()
        self._calls.append(time.monotonic())


API_RATE_LIMITER = RateLimiter()

# ---------------------------------------------------------------------------
# HTTP cache session
# ---------------------------------------------------------------------------

# Global HTTP cache session (initialized lazily)
_HTTP_SESSION: requests_cache.CachedSession | None = None
_CACHE_ENABLED = True


def _get_http_session() -> requests_cache.CachedSession:
    """Get or create the global HTTP cache session.

    All responses are cached for 7 days (604800 seconds).
    Uses filesystem backend to handle large responses (>2GB).

    Returns:
        CachedSession: requests-cache session with 7-day expiration.
    """
    global _HTTP_SESSION

    if _HTTP_SESSION is None:
        cache_path = str(get_http_cache_path())

        _HTTP_SESSION = requests_cache.CachedSession(
            cache_name=cache_path,
            backend="filesystem",
            expire_after=604800,  # 7 days
            allowable_codes=(200, 404),  # Cache 404s for version fallback
            stale_if_error=True,  # Use stale cache if API errors
        )

    return _HTTP_SESSION


def get_response_text(url: str, headers: dict) -> tuple[int, str, bool]:
    """GET request returning status code, text content, and cache hit status.

    This call is subject to the global rate limiter and HTTP caching.

    Args:
        url: The URL to fetch.
        headers: Headers to include in the request.

    Returns:
        tuple[int, str, bool]: Status code, text content, and whether from cache.
    """
    API_RATE_LIMITER.wait()

    if _CACHE_ENABLED:
        session = _get_http_session()
        response = session.get(url, headers=headers)
        from_cache = getattr(response, "from_cache", False)
        if from_cache:
            logger.info(f"Loading data from HTTP cache: {url}")
        else:
            logger.info(f"Fetching data from API: {url}")
    else:
        response = requests.get(url, headers=headers)
        from_cache = False
        logger.info(f"Fetching data from API (cache disabled): {url}")

    return response.status_code, response.text, from_cache


def get_response_content(url: str, headers: dict) -> tuple[int, bytes, bool]:
    """GET request returning status code, raw content, and cache hit status.

    This call is subject to the global rate limiter and HTTP caching.

    Args:
        url: The URL to fetch.
        headers: Headers to include in the request.

    Returns:
        tuple[int, bytes, bool]: Status code, content bytes, and whether from cache.
    """
    API_RATE_LIMITER.wait()

    if _CACHE_ENABLED:
        session = _get_http_session()
        response = session.get(url, headers=headers)
        from_cache = getattr(response, "from_cache", False)
        if from_cache:
            logger.info(f"Loading data from HTTP cache: {url}")
        else:
            logger.info(f"Fetching data from API: {url}")
    else:
        response = requests.get(url, headers=headers)
        from_cache = False
        logger.info(f"Fetching data from API (cache disabled): {url}")

    return response.status_code, response.content, from_cache
