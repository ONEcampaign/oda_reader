"""Typed exceptions for the oda_reader boundary contract."""

import zipfile
import zlib
from pathlib import Path

BULK_PAYLOAD_CORRUPT_HINT = (
    "Call the bulk_download function again to refetch (the corrupt entry "
    "has been removed), run oda_data.cache.clear('raw') to wipe the raw "
    "cache, or call with use_raw_cache=False to bypass."
)

_HTTP_BODY_PREVIEW = 500


class BulkPayloadCorruptError(Exception):
    """Raised when a downloaded bulk payload fails integrity validation.

    Attributes:
        path: The path of the failed cache entry. The entry has already
            been removed from disk by the time this exception is raised.
        reason: A short human-readable description of which check failed
            (e.g. "is_zipfile() returned False",
            "testzip() reported member 'crs.parquet'").
    """

    def __init__(self, path: Path, *, reason: str) -> None:
        self.path: Path = path
        self.reason: str = reason
        super().__init__(
            f"Cached payload at {path} failed integrity validation "
            f"({reason}). {BULK_PAYLOAD_CORRUPT_HINT}"
        )


class BulkDownloadHTTPError(ConnectionError):
    """Raised when a bulk download HTTP request returns a non-2xx status.

    Subclasses ``ConnectionError`` for backward compatibility with callers
    that catch the previous untyped exception.

    Attributes:
        status_code: The HTTP status code returned by the server.
        url: The URL that was requested.
        body: A truncated preview of the response body (max 500 chars).
    """

    def __init__(self, *, status_code: int, url: str, body: str) -> None:
        self.status_code = status_code
        self.url = url
        self.body = body[:_HTTP_BODY_PREVIEW]
        super().__init__(f"HTTP {status_code} from {url}: {self.body}")


def validate_zip_or_raise(path: Path) -> None:
    """Validate a zip file with is_zipfile + testzip; on failure unlink and raise.

    Any exception that ``testzip()`` itself raises (BadZipFile from a damaged
    central directory, zlib.error from a corrupt compressed member) is
    converted into BulkPayloadCorruptError so callers see a single boundary
    exception and the corrupt file is always removed.

    Args:
        path: Path to the zip file to validate.

    Raises:
        BulkPayloadCorruptError: If the file fails either check. The file is
            unlinked before raising so callers can simply retry.
    """
    if not zipfile.is_zipfile(path):
        path.unlink(missing_ok=True)
        raise BulkPayloadCorruptError(path, reason="is_zipfile() returned False")
    try:
        with zipfile.ZipFile(path) as zf:
            bad_member = zf.testzip()
    except (zipfile.BadZipFile, zlib.error) as e:
        path.unlink(missing_ok=True)
        raise BulkPayloadCorruptError(
            path, reason=f"testzip() raised {type(e).__name__}: {e}"
        ) from e
    if bad_member is not None:
        path.unlink(missing_ok=True)
        raise BulkPayloadCorruptError(
            path, reason=f"testzip() reported member {bad_member!r}"
        )
