import logging
import re
from copy import deepcopy
from io import StringIO
from pathlib import Path
import time
from collections import deque
from typing import Optional

import pandas as pd
import requests
import requests_cache

from oda_reader._cache.config import get_http_cache_path

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

logger = logging.getLogger("oda_importer")

FALLBACK_STEP = 0.1
MAX_RETRIES = 5


# Global HTTP cache session (initialized lazily)
_HTTP_SESSION: Optional[requests_cache.CachedSession] = None
_CACHE_ENABLED = True


def _get_http_session() -> requests_cache.CachedSession:
    """Get or create the global HTTP cache session.

    All responses are cached for 7 days (604800 seconds).

    Returns:
        CachedSession: requests-cache session with 7-day expiration.
    """
    global _HTTP_SESSION

    if _HTTP_SESSION is None:
        cache_path = str(get_http_cache_path())

        _HTTP_SESSION = requests_cache.CachedSession(
            cache_name=cache_path,
            backend="sqlite",
            expire_after=604800,  # 7 days
            allowable_codes=(200, 404),  # Cache 404s for version fallback
            stale_if_error=True,  # Use stale cache if API errors
        )

    return _HTTP_SESSION


def enable_http_cache() -> None:
    """Enable HTTP response caching.

    Example:
        >>> from oda_reader import enable_http_cache
        >>> enable_http_cache()
    """
    global _CACHE_ENABLED
    _CACHE_ENABLED = True
    if _HTTP_SESSION is not None:
        _HTTP_SESSION.cache.clear()  # Clear any cached data from disabled session


def disable_http_cache() -> None:
    """Disable HTTP response caching (useful for testing or forcing fresh data).

    Example:
        >>> from oda_reader import disable_http_cache
        >>> disable_http_cache()
    """
    global _CACHE_ENABLED
    _CACHE_ENABLED = False


def clear_http_cache() -> None:
    """Clear all cached HTTP responses.

    Example:
        >>> from oda_reader import clear_http_cache
        >>> clear_http_cache()
    """
    session = _get_http_session()
    session.cache.clear()
    logger.info("HTTP cache cleared")


def get_http_cache_info() -> dict:
    """Get information about the HTTP cache.

    Returns:
        Dict with cache statistics (response_count, redirects, etc.).

    Example:
        >>> from oda_reader import get_http_cache_info
        >>> info = get_http_cache_info()
        >>> print(f"Cached responses: {info['response_count']}")
    """
    session = _get_http_session()
    return {
        "response_count": len(session.cache.responses),
        "redirects_count": len(session.cache.redirects),
    }


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


class ImporterPaths:
    """Class to store the paths to the data and output folders."""

    project = Path(__file__).resolve().parent.parent
    scripts = project / "oda_reader"
    schemas = scripts / "schemas"
    mappings = schemas / "mappings"
    cache = scripts / ".cache"


def text_to_stringIO(response_text: str) -> StringIO:
    """Convert the content of a response to bytes.

    Args:
        response (str): The response text from the API.

    Returns:
        StringIO: The content of the response as a stringIO object.

    """
    # Use BytesIO to handle the binary stream data
    return StringIO(response_text)


def _replace_dataflow_version(url: str, version: str) -> str:
    """Replace the dataflow version in the URL."""
    pattern = r",(\d+\.\d+)/"

    return re.sub(pattern, f",{version}/", url)


def _get_dataflow_version(url: str) -> str:
    """Get the dataflow version from the URL."""
    pattern = r",(\d+\.\d+)/"

    return re.search(pattern, url).group(1)


def _get_response_text(url: str, headers: dict) -> tuple[int, str, bool]:
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


def _get_response_content(url: str, headers: dict) -> tuple[int, bytes, bool]:
    """GET request returning status code, content, and cache hit status.

    This call is subject to the global rate limiter and HTTP caching.

    Args:
        url: The URL to fetch.
        headers: Headers to include in the request.

    Returns:
        tuple[int, bytes, bool]: Status code, content, and whether from cache.
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


def get_data_from_api(url: str, compressed: bool = True, retries: int = 0) -> str:
    """Download a CSV file from an API endpoint and return it as text.

    Args:
        url: The URL of the API endpoint.
        compressed: Whether the data is fetched compressed. Strongly recommended.
        retries: The number of retries to attempt.

    Returns:
        str: The response text from the API.
    """
    # Set the headers with gzip encoding (if required)
    if compressed:
        headers = {"Accept-Encoding": "gzip"}
    else:
        headers = {}

    # Fetch the data with headers
    status_code, response, from_cache = _get_response_text(url, headers=headers)

    if (status_code == 404) and (
        ("Dataflow" in response) or (response == "NoRecordsFound")
    ):
        if retries < MAX_RETRIES:
            version = _get_dataflow_version(url)
            new_version = str(round(float(version) - FALLBACK_STEP, 1))
            new_url = _replace_dataflow_version(url=url, version=new_version)
            return get_data_from_api(
                url=new_url, compressed=compressed, retries=retries + 1
            )
        else:
            raise ConnectionError("No data found for the selected parameters.")

    if (status_code == 500) and (response.find("not set to") > 0):
        url = url.replace("public", "dcd-public")
        status_code, response, from_cache = _get_response_text(url, headers=headers)

    if status_code > 299:
        logger.error(f"Error {status_code}: {response}")
        raise ConnectionError(f"Error {status_code}: {response}")

    return response


def api_response_to_df(
    url: str, read_csv_options: dict = None, compressed: bool = True
) -> pd.DataFrame:
    """Download a CSV file from an API endpoint and return it as a DataFrame.

    Args:
        url (str): The URL of the API endpoint.
        read_csv_options (dict): Options to pass to `pd.read_csv`.
        compressed (bool): Whether the data is fetched compressed. Strongly recommended.

    Returns:
        pd.DataFrame: The data as a DataFrame.

    """
    # Set default options for read_csv
    if read_csv_options is None:
        read_csv_options = {}

    # If asked for uncompressed data, return the data as is
    if not compressed:
        return pd.read_csv(url, **read_csv_options)

    # Fetch the data from the API with compression headers
    response = get_data_from_api(url=url, compressed=compressed)

    # Convert the content to stringIO
    data = text_to_stringIO(response)

    # Return the data as a DataFrame
    try:
        d_ = deepcopy(data)
        return pd.read_csv(d_, **read_csv_options)
    except ValueError:
        read_csv_options["dtype"]["CHANNEL"] = "string[pyarrow]"
        read_csv_options["dtype"]["MODALITY"] = "string[pyarrow]"
        read_csv_options["dtype"]["MD_DIM"] = "string[pyarrow]"
        return pd.read_csv(data, **read_csv_options)
