"""Typed exceptions for the oda_reader boundary contract."""

import re
import typing
import zipfile
import zlib
from html.parser import HTMLParser
from pathlib import Path

BULK_PAYLOAD_CORRUPT_HINT = (
    "Call the bulk_download function again to refetch (the corrupt entry "
    "has been removed), run oda_data.cache.clear('raw') to wipe the raw "
    "cache, or call with use_raw_cache=False to bypass."
)

_HTTP_BODY_PREVIEW = 500
_CODELIST_BODY_PREVIEW = 64 * 1024  # bytes


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


# --- oda_reader.codelists exceptions -----------------------------------


class _HiddenTokenFinder(HTMLParser):
    """Collects the exact source text of every ASP.NET hidden-token input.

    An ASP.NET hidden token is a ``<input type="hidden">`` whose ``name``
    starts with ``__`` (``__VIEWSTATE``, ``__EVENTVALIDATION``, and so on).
    Only the raw tag text is kept, so redaction can do a targeted
    string-replace on that exact substring without touching surrounding
    markup.
    """

    def __init__(self) -> None:
        super().__init__()
        self.tags: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag != "input":
            return
        attr_map = dict(attrs)
        if (attr_map.get("type") or "").lower() != "hidden":
            return
        name = attr_map.get("name") or ""
        if not name.startswith("__"):
            return
        raw_tag = self.get_starttag_text()
        if raw_tag is not None:
            self.tags.append(raw_tag)


# Matches a `value` attribute in any of its three valid HTML forms:
# double-quoted, single-quoted, or unquoted. IGNORECASE because attribute
# names are case-insensitive in HTML (`VALUE=`, `Value=` are both legal).
# The leading `(?<![\w-])` is a left boundary: without it "value" also
# matches inside `data-value=` or any other attribute name that merely ends
# in "value" (`\b` does not help here -- "-" is a non-word character, so
# `\bvalue` still matches inside "data-value").
_VALUE_ATTR_RE = re.compile(
    r'(?<![\w-])value\s*=\s*(?:"[^"]*"|\'[^\']*\'|[^\s>]*)', re.IGNORECASE
)
# Same match, but with the value's own text captured, used only to verify
# post-substitution that no original value text survived redaction.
_VALUE_CONTENT_RE = re.compile(
    r'(?<![\w-])value\s*=\s*(?:"([^"]*)"|\'([^\']*)\'|([^\s>]*))', re.IGNORECASE
)


def _captured_values(raw_tag: str) -> list[str]:
    """Every value-attribute's text in *raw_tag*, whichever quote style matched."""
    return [
        next((g for g in match.groups() if g), "")
        for match in _VALUE_CONTENT_RE.finditer(raw_tag)
    ]


def _redact_hidden_token_values(body: bytes) -> bytes:
    """Replace the ``value`` of every hidden ASP.NET token input with a placeholder.

    Removes live ``__VIEWSTATE`` (or similar hidden token) values from any
    body an exception is about to carry, so they never reach a log aggregator
    or an issue tracker. A no-op on non-HTML bodies, such as a JSON envelope
    failure, since no hidden-input tag can match.

    Args:
        body: The raw response bytes to redact.

    Returns:
        The bytes with every hidden ``__``-prefixed input's value replaced
        by a placeholder derived from its name. If parsing itself raises,
        the original bytes are withheld -- a bounded placeholder is
        returned instead, since fanning out the unparsed body would be the
        one path that could leak a live token unredacted. The same fail-closed
        rule applies per tag: if a tag is recognised as carrying a
        ``__``-prefixed name but its value cannot be located and substituted
        (no ``value`` attribute at all, or an unrecognised form), the whole
        tag is withheld rather than emitted with a live value still attached.
    """
    text = body.decode("utf-8", errors="replace")
    finder = _HiddenTokenFinder()
    try:
        finder.feed(text)
    except Exception:
        return b"[body withheld: redaction failed to parse it]"
    for raw_tag in finder.tags:
        # IGNORECASE: `NAME="__VIEWSTATE"` is valid HTML, and attribute
        # names are case-insensitive generally -- the parser lowercases
        # them for `_HiddenTokenFinder`'s own matching, but this regex runs
        # over the tag's original-case raw text.
        name_match = re.search(
            r'name\s*=\s*["\']?(__[^"\'\s>]*)', raw_tag, flags=re.IGNORECASE
        )
        if name_match is None:
            continue
        placeholder = f"PLACEHOLDER_{name_match.group(1)}"
        original_values = _captured_values(raw_tag)
        # A replacement function, not a string: raw_tag is untrusted server
        # content, and a string replacement would let a backslash in
        # placeholder (i.e. in the token's own name) be misread as a regex
        # backreference by re.sub. No count= limit: a malformed tag can
        # carry more than one value-shaped attribute (a duplicate `value=`,
        # or another attribute merely ending in "value" that still slips
        # past the boundary above in some form we didn't anticipate), and
        # every one of them must be substituted, not just the first.
        redacted_tag, substitutions = _VALUE_ATTR_RE.subn(
            lambda _m, _p=placeholder: f'value="{_p}"', raw_tag
        )
        # The guarantee is "no live token text survives in this tag", and
        # counting substitutions is only a proxy for that -- so verify the
        # outcome directly: none of the values captured before substitution
        # may still appear anywhere in the result. Recognised as a hidden
        # __-prefixed token but the value couldn't be safely eliminated --
        # withhold the whole tag rather than risk emitting a live value in
        # a form the regex above didn't anticipate. Fail closed, matching
        # the parse-failure path above.
        if substitutions == 0 or any(
            value and value in redacted_tag for value in original_values
        ):
            redacted_tag = (
                f"<!-- [tag withheld: could not redact {name_match.group(1)}] -->"
            )
        text = text.replace(raw_tag, redacted_tag)
    return text.encode("utf-8")


def _prepare_body(body: bytes | None) -> bytes | None:
    """Redact hidden ASP.NET tokens, then bound the result to 64 KiB.

    Applied to every ``body`` an exception stores, at construction time, so
    a ``stage="hidden-field"`` failure never carries a live ``__VIEWSTATE``
    outward, and so no exception ever holds an unbounded response.
    """
    if body is None:
        return None
    return _redact_hidden_token_values(body)[:_CODELIST_BODY_PREVIEW]


class CodelistError(Exception):
    """Base for every failure raised by oda_reader.codelists.

    All five codelist exceptions inherit from ``Exception`` directly, via
    this base, rather than from a builtin like ``ConnectionError`` or
    ``OSError``. ``BulkDownloadHTTPError``'s ``ConnectionError`` base is a
    migration accommodation for callers of a previous untyped exception; a
    new class has no such callers to accommodate. And the builtin is an
    ``OSError``, so a consumer's ``except OSError`` guarding a fetch
    followed by a file write would otherwise swallow a codelist failure as
    a disk problem.

    Attributes:
        is_retryable: True only on CodelistFetchError. Lets a caller branch
            without knowing the subclass list.
        codelist_id: Which codelist was being handled, or None before that
            is known.
    """

    is_retryable: bool = False

    def __init__(self, *, message: str, codelist_id: str | None = None) -> None:
        self.codelist_id = codelist_id
        super().__init__(message)


class CodelistFetchError(CodelistError):
    """The OECD codelist app could not be reached, or failed transiently.

    Retryable. Raised after all internal retries are exhausted.

    Attributes:
        url: The URL that was being requested.
        stage: Which step of the handshake failed: fetching the page,
            selecting the codelist, or downloading the payload.
        attempts: Total attempts made, including the one that raised.
        status_code: The HTTP status code of the final attempt, or None for
            a transport-level failure (timeout, connection reset).
        codelist_id: Which codelist was being handled, or None.
    """

    is_retryable = True

    def __init__(
        self,
        *,
        url: str,
        stage: typing.Literal["page", "select", "download"],
        attempts: int,
        status_code: int | None = None,
        codelist_id: str | None = None,
    ) -> None:
        self.url = url
        self.stage = stage
        self.attempts = attempts
        self.status_code = status_code
        message = (
            f"Codelist fetch failed at {stage} stage after {attempts} attempt(s): {url}"
        )
        if status_code is not None:
            message += f" (last status {status_code})"
        if codelist_id is not None:
            message += f" [codelist_id={codelist_id}]"
        super().__init__(message=message, codelist_id=codelist_id)


class CodelistSourceError(CodelistError):
    """The source is gone, has moved, or is refusing us.

    Not retryable. Raised for a 404, a 403, an unexpected redirect, or a
    200 response that is plainly not CodesList.aspx (a maintenance or
    error page).

    Attributes:
        url: The URL that was requested.
        stage: Which step of the handshake produced the failure: fetching
            the page, selecting the codelist, or downloading the payload.
        status_code: The HTTP status code, or None (e.g. a refused
            redirect has no meaningful status here).
        final_url: The URL actually reached, if it differs from url.
        location: The Location header of a refused redirect, or None.
        body: A bounded (64 KiB), token-redacted preview of the response
            bytes, or None.
        codelist_id: Which codelist was being handled, or None.
    """

    def __init__(
        self,
        *,
        url: str,
        stage: typing.Literal["page", "select", "download"],
        status_code: int | None = None,
        final_url: str | None = None,
        location: str | None = None,
        body: bytes | None = None,
        codelist_id: str | None = None,
    ) -> None:
        self.url = url
        self.stage = stage
        self.status_code = status_code
        self.final_url = final_url
        self.location = location
        self.body = _prepare_body(body)
        message = f"Codelist source error at {stage} stage: {url}"
        if status_code is not None:
            message += f" (status {status_code})"
        if location is not None:
            message += f" (redirected to {location})"
        if codelist_id is not None:
            message += f" [codelist_id={codelist_id}]"
        super().__init__(message=message, codelist_id=codelist_id)


class CodelistShapeError(CodelistError):
    """We obtained our document and its structure is not what we expect.

    Not retryable. A human must look at the page or the payload.

    Attributes:
        stage: Which part of the shape check failed: a required hidden
            field, the JSON envelope, or a row within it.
        detail: What specifically was wrong.
        body: A bounded (64 KiB), token-redacted preview of the offending
            bytes, or None.
        url: The handshake URL the offending document came from, or None.
        codelist_id: Which codelist was being handled, or None.
    """

    def __init__(
        self,
        *,
        stage: typing.Literal["hidden-field", "envelope", "row"],
        detail: str,
        body: bytes | None = None,
        url: str | None = None,
        codelist_id: str | None = None,
    ) -> None:
        self.stage = stage
        self.detail = detail
        self.body = _prepare_body(body)
        self.url = url
        message = f"Codelist shape error at {stage} stage: {detail}"
        if url is not None:
            message += f" ({url})"
        if codelist_id is not None:
            message += f" [codelist_id={codelist_id}]"
        super().__init__(message=message, codelist_id=codelist_id)


class CodelistValidationError(CodelistError):
    """We parsed the input and the result cannot be a valid codelist, or the
    caller handed us something we cannot honour.

    Not retryable. Covers absolute self-consistency gates (e.g. zero rows
    for a requested codelist) and argument validation.

    Attributes:
        detail: Which gate failed, with the observed values.
        codelist_id: Which codelist was being handled, or None.
    """

    def __init__(self, *, detail: str, codelist_id: str | None = None) -> None:
        self.detail = detail
        message = f"Codelist validation error: {detail}"
        if codelist_id is not None:
            message += f" [codelist_id={codelist_id}]"
        super().__init__(message=message, codelist_id=codelist_id)
