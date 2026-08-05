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
from collections.abc import Callable
from typing import cast
from urllib.parse import urljoin, urlsplit

import requests
import requests_cache

from oda_reader._cache.config import get_http_cache_path

logger = logging.getLogger("oda_importer")

# ---------------------------------------------------------------------------
# Same-origin redirect following
# ---------------------------------------------------------------------------

# `requests` follows redirects transparently by default, so a compromised or
# misbehaving server could hand a request off to an arbitrary off-origin
# host without anything here noticing. `get_response_text`/
# `get_response_content` are used against many different hosts (OECD's SDMX
# API, the AidData Excel download, ...), so there's no single host
# allowlist to check against here the way download_tools.py's bulk-file
# fetch has -- same-origin is the invariant that holds for all of them.
_MAX_REDIRECT_HOPS = 5


def _same_origin(url: str, *, reference: str) -> bool:
    """True if *url* shares scheme and host with *reference*."""
    a, b = urlsplit(url), urlsplit(reference)
    return (a.scheme, a.netloc) == (b.scheme, b.netloc)


def _get_same_origin(
    getter: Callable[..., requests.Response],
    url: str,
    headers: dict,
    **kwargs: object,
) -> requests.Response:
    """GET *url* via *getter*, following only same-origin redirects.

    *getter* is a bound `session.get` or the module-level `requests.get`,
    called with `allow_redirects=False` so every hop can be checked before
    being followed -- otherwise a same-host URL that server-side-redirects
    to a different host would be followed silently. A cross-origin
    redirect, or a same-origin chain that exceeds `_MAX_REDIRECT_HOPS`, is
    returned as-is, unfollowed; callers already treat a 3xx status like any
    other non-2xx response, so no separate error path is needed here.

    Args:
        getter: `session.get` or `requests.get`.
        url: The URL to fetch.
        headers: Headers to send with every hop.
        **kwargs: Extra keyword arguments passed through to every hop (e.g.
            `force_refresh` for a `CachedSession`).

    Returns:
        requests.Response: The terminal response -- either non-redirect, or
            an unfollowed redirect that failed the same-origin/hop-cap check.
    """
    origin = url
    for hop in range(_MAX_REDIRECT_HOPS + 1):
        response = getter(url, headers=headers, allow_redirects=False, **kwargs)
        status = response.status_code
        if status is None or not (300 <= status < 400):
            return response
        location = response.headers.get("Location")
        if not location or hop == _MAX_REDIRECT_HOPS:
            return response
        target = urljoin(url, location)
        if not _same_origin(target, reference=origin):
            return response
        url = target
    return response  # pragma: no cover -- loop above always returns first


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
# Bulk-download headers and session
# ---------------------------------------------------------------------------

# webfs-dcd.oecd.org (the bulk file host) answers a plain requests-style
# header set with a Cloudflare challenge (403, cf-mitigated: challenge). A
# browser-like header set reliably clears it (verified 6/6 trials). This is
# a pragmatic unblock for a public download endpoint, not a durable
# contract -- if Cloudflare tightens further, this will need to change again.
DEFAULT_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,"
        "image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "sec-ch-ua": '"Chromium";v="131", "Not_A Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
}

# A plain (uncached) session for streaming bulk-download bodies, distinct
# from the CachedSession below: a 1+ GB body must never enter the HTTP
# cache. Reusing one Session still buys connection-pooling across the retry
# attempts in download_tools._stream_to_file. It intentionally does not
# carry a __cf_bm cookie over from the annotation fetch -- that cookie is
# host-scoped, the annotation fetch hits sdmx.oecd.org through the
# CachedSession below while this hits webfs-dcd.oecd.org, and there was
# never a cross-host cookie dependency to preserve.
_BULK_STREAM_SESSION: requests.Session | None = None


def _get_bulk_stream_session() -> requests.Session:
    """Get or create the module-level plain session used for bulk file streams."""
    global _BULK_STREAM_SESSION
    if _BULK_STREAM_SESSION is None:
        _BULK_STREAM_SESSION = requests.Session()
    return _BULK_STREAM_SESSION


# ---------------------------------------------------------------------------
# HTTP cache session
# ---------------------------------------------------------------------------

# Global HTTP cache session (initialized lazily)
_HTTP_SESSION: requests_cache.CachedSession | None = None
_CACHE_ENABLED: bool = True


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


def get_response_text(
    url: str, headers: dict, *, force_refresh: bool = False
) -> tuple[int, str, bool]:
    """GET request returning status code, text content, and cache hit status.

    This call is subject to the global rate limiter and HTTP caching.

    Args:
        url: The URL to fetch.
        headers: Headers to include in the request.
        force_refresh: If True, bypass any cached response for this URL and
            always hit the network, overwriting the cache entry with the
            fresh response. Used to recover from a stale cached response
            that keeps pointing at a dead resource between the cache's
            normal expiry.

    Returns:
        tuple[int, str, bool]: Status code, text content, and whether from cache.
    """
    API_RATE_LIMITER.wait()

    if _CACHE_ENABLED:
        session = _get_http_session()
        response = _get_same_origin(
            session.get, url, headers, force_refresh=force_refresh
        )
        from_cache = getattr(response, "from_cache", False)
        if from_cache:
            logger.info(f"Loading data from HTTP cache: {url}")
        else:
            logger.info(f"Fetching data from API: {url}")
    else:
        response = _get_same_origin(requests.get, url, headers)
        from_cache = False
        logger.info(f"Fetching data from API (cache disabled): {url}")

    return cast(int, response.status_code), response.text, from_cache


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
        response = _get_same_origin(session.get, url, headers)
        from_cache = getattr(response, "from_cache", False)
        if from_cache:
            logger.info(f"Loading data from HTTP cache: {url}")
        else:
            logger.info(f"Fetching data from API: {url}")
    else:
        response = _get_same_origin(requests.get, url, headers)
        from_cache = False
        logger.info(f"Fetching data from API (cache disabled): {url}")

    return cast(int, response.status_code), response.content, from_cache
