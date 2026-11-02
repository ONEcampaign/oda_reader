"""Fallback mirror of OECD bulk files at bulk.developmentfinance.org.

The mirror holds byte-identical copies of OECD's bulk files, keyed by OECD's
URL and stamped with OECD's own version token. A manifest at
``<base>manifest.json`` lists them. This module holds the mirror's
configuration, the manifest model, the manifest fetch, the rule deciding when
a mirror copy may replace a cached file, and the checks applied to a mirror
download. ``download_tools._stream_to_file`` streams the bytes. This module
imports nothing from ``download_tools``, so that module can import this one.
"""

import hashlib
import json
import logging
import os
import re
import threading
import time
import typing
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import unquote, urlsplit

import requests

from oda_reader._cache.manager import ISO_FORMAT
from oda_reader._http_primitives import DEFAULT_HEADERS, _get_bulk_stream_session

logger = logging.getLogger(__name__)

MIRROR_ENV_VAR = "ODA_READER_BULK_MIRROR"
DEFAULT_MIRROR_BASE_URL = "https://bulk.developmentfinance.org/oecd/"

# The base URL serves this prefix of the bucket: the object `oecd/a/b.zip`
# is at `<base>a/b.zip`, and the manifest is `oecd/manifest.json`.
OBJECT_PREFIX = "oecd/"
MANIFEST_NAME = "manifest.json"
MANIFEST_KEY = OBJECT_PREFIX + MANIFEST_NAME
MANIFEST_SCHEMA = 1

# The Worker serves the manifest with `max-age=300`, and a process re-reads it
# no more often than that. A failed fetch is remembered as long, so a mirror
# outage costs one timeout and one log line per window, not one per dataset.
_MANIFEST_TTL_SECONDS = 300
_MANIFEST_TIMEOUT = (10, 30)
_MANIFEST_MAX_BYTES = 4 * 1024 * 1024

# An object key is `oecd/<dataset>/<sha256><ext>`. The digest in the name must
# match the entry's `sha256`, since objects are content-addressed.
_OBJECT_KEY_RE = re.compile(
    r"oecd/[A-Za-z0-9_-]+/(?P<sha256>[0-9a-f]{64})(?:\.[A-Za-z0-9]+)*"
)
_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

# The header a file host's version token came from, ETag or else Last-Modified.
# `download_tools` imports it from here, since this module must not import
# `download_tools`.
type TokenKind = typing.Literal["etag", "last_modified"]
_TOKEN_KINDS: tuple[TokenKind, ...] = ("etag", "last_modified")


class MirrorManifestError(ValueError):
    """The mirror manifest is malformed or has an unsupported schema."""


class MirrorIntegrityError(Exception):
    """A file downloaded from the mirror does not match its manifest entry."""


@dataclass(frozen=True, slots=True)
class MirrorFile:
    """One manifest entry: a mirrored copy of one OECD bulk file.

    Attributes:
        token: OECD's version token for these bytes (ETag or Last-Modified).
        token_kind: The header *token* came from.
        label_release: The release id OECD's annotation named when the bytes
            were fetched, or None when the downloaded token differed from the
            probed one.
        object: The bucket key, ``oecd/<dataset>/<sha256><ext>``.
        size: The file's size in bytes.
        sha256: The file's SHA-256, lowercase hex.
        checked_at: The last time the refresher saw OECD serve *token*.
    """

    token: str
    token_kind: TokenKind
    label_release: str | None
    object: str
    size: int
    sha256: str
    checked_at: datetime


@dataclass(frozen=True, slots=True)
class MirrorManifest:
    """The mirror's manifest.

    Attributes:
        generated_at: When the refresher wrote the manifest.
        attribution: The OECD attribution text for the mirrored files.
        files: Entries keyed by OECD URL in its percent-encoded form, as
            ``download_tools._encode_url_path`` returns it.
    """

    generated_at: datetime
    attribution: str
    files: dict[str, MirrorFile]


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


def mirror_base_url() -> str | None:
    """Return the configured mirror base URL, or None when the mirror is off.

    ``ODA_READER_BULK_MIRROR`` unset or empty selects
    `DEFAULT_MIRROR_BASE_URL`. ``off`` (any case) disables the mirror. Any
    other value is the base URL. A trailing slash is added when missing.

    Raises:
        ValueError: If the configured base URL is not ``https`` with a host,
            or carries a query or fragment.
    """
    value = os.environ.get(MIRROR_ENV_VAR, "").strip()
    if not value:
        return DEFAULT_MIRROR_BASE_URL
    if value.casefold() == "off":
        return None

    parsed = urlsplit(value)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError(
            f"{MIRROR_ENV_VAR} must be 'off' or an https base URL with no query "
            f"or fragment, got {value!r}."
        )
    return value if value.endswith("/") else value + "/"


def manifest_url(*, base: str) -> str:
    """Return the manifest URL under *base*."""
    return base + MANIFEST_NAME


def object_url(entry: MirrorFile, *, base: str) -> str:
    """Return the URL serving *entry*'s object under *base*."""
    return base + entry.object.removeprefix(OBJECT_PREFIX)


def mirror_url_validator(*, base: str) -> Callable[[str], None]:
    """Return a URL check for fetches from the mirror at *base*.

    The check suits the ``validate`` parameter of
    ``download_tools._stream_to_file``. It accepts an ``https`` URL on
    *base*'s host and port whose path sits under *base*'s path, with no
    ``.`` or ``..`` segment, and raises ``ValueError`` for anything else.
    It is applied to every redirect hop, so a redirect off the mirror fails.
    """
    expected = urlsplit(base)

    def validate(url: str) -> None:
        parsed = urlsplit(url)
        segments = unquote(parsed.path).split("/")
        if (
            parsed.scheme != "https"
            or parsed.netloc != expected.netloc
            or not parsed.path.startswith(expected.path)
            or "." in segments
            or ".." in segments
        ):
            raise ValueError(
                f"Refusing to fetch {url!r}: it is outside the mirror {base!r}."
            )

    return validate


# ---------------------------------------------------------------------------
# Manifest model
# ---------------------------------------------------------------------------


def parse_manifest(text: str | bytes) -> MirrorManifest:
    """Parse and validate manifest JSON.

    Every documented field must be present with its documented type. Fields
    the schema does not name are ignored, so a later writer can add one
    without breaking this reader. A breaking change bumps ``schema``.

    Raises:
        MirrorManifestError: If the JSON is malformed, the schema is not
            `MANIFEST_SCHEMA`, or any field is missing or invalid.
    """
    try:
        data = json.loads(text)
    except ValueError as e:
        raise MirrorManifestError(f"Manifest is not valid JSON: {e}") from e
    if not isinstance(data, dict):
        raise MirrorManifestError("Manifest is not a JSON object.")

    schema = data.get("schema")
    if schema != MANIFEST_SCHEMA or isinstance(schema, bool):
        raise MirrorManifestError(
            f"Unsupported manifest schema {schema!r}; expected {MANIFEST_SCHEMA}."
        )
    files = _field(data, "files", dict, where="manifest")
    return MirrorManifest(
        generated_at=_parse_timestamp(data, "generated_at", where="manifest"),
        attribution=_field(data, "attribution", str, where="manifest"),
        files={
            _check_url_key(url): _parse_file(url, raw) for url, raw in files.items()
        },
    )


def dump_manifest(manifest: MirrorManifest) -> str:
    """Serialise *manifest* to the JSON `parse_manifest` reads.

    Keys are sorted so two runs that change nothing write identical text.
    """
    data = {
        "schema": MANIFEST_SCHEMA,
        "generated_at": _format_timestamp(manifest.generated_at),
        "attribution": manifest.attribution,
        "files": {
            url: {
                "token": entry.token,
                "token_kind": entry.token_kind,
                "label_release": entry.label_release,
                "object": entry.object,
                "size": entry.size,
                "sha256": entry.sha256,
                "checked_at": _format_timestamp(entry.checked_at),
            }
            for url, entry in manifest.files.items()
        },
    }
    return json.dumps(data, indent=2, sort_keys=True) + "\n"


def _parse_file(url: str, raw: object) -> MirrorFile:
    where = f"entry {url!r}"
    if not isinstance(raw, dict):
        raise MirrorManifestError(f"Manifest {where} is not a JSON object.")

    token = _field(raw, "token", str, where=where)
    if not token:
        raise MirrorManifestError(f"Manifest {where} has an empty token.")
    token_kind = _field(raw, "token_kind", str, where=where)
    if token_kind not in _TOKEN_KINDS:
        raise MirrorManifestError(
            f"Manifest {where} has token_kind {token_kind!r}; "
            f"expected one of {_TOKEN_KINDS}."
        )
    label_release = raw.get("label_release")
    if "label_release" not in raw or not isinstance(label_release, str | None):
        raise MirrorManifestError(
            f"Manifest {where} needs label_release as a string or null."
        )
    size = _field(raw, "size", int, where=where)
    if isinstance(size, bool) or size < 0:
        raise MirrorManifestError(f"Manifest {where} has invalid size {size!r}.")
    sha256 = _field(raw, "sha256", str, where=where)
    if not _SHA256_RE.fullmatch(sha256):
        raise MirrorManifestError(f"Manifest {where} has invalid sha256 {sha256!r}.")
    obj = _field(raw, "object", str, where=where)
    match = _OBJECT_KEY_RE.fullmatch(obj)
    if match is None or match["sha256"] != sha256:
        raise MirrorManifestError(
            f"Manifest {where} has object {obj!r}; expected "
            f"'oecd/<dataset>/{sha256}<ext>'."
        )

    return MirrorFile(
        token=token,
        token_kind=typing.cast(TokenKind, token_kind),
        label_release=label_release,
        object=obj,
        size=size,
        sha256=sha256,
        checked_at=_parse_timestamp(raw, "checked_at", where=where),
    )


def _field[T](data: dict, name: str, kind: type[T], *, where: str) -> T:
    value = data.get(name)
    if not isinstance(value, kind):
        raise MirrorManifestError(
            f"Manifest {where} needs {name!r} as {kind.__name__}, got {value!r}."
        )
    return value


def _check_url_key(url: str) -> str:
    parsed = urlsplit(url)
    if parsed.scheme != "https" or not parsed.hostname:
        raise MirrorManifestError(f"Manifest key {url!r} is not an https URL.")
    return url


def _parse_timestamp(data: dict, name: str, *, where: str) -> datetime:
    raw = _field(data, name, str, where=where)
    try:
        value = datetime.fromisoformat(raw)
    except ValueError as e:
        raise MirrorManifestError(
            f"Manifest {where} has invalid {name} {raw!r}."
        ) from e
    if value.tzinfo is None:
        raise MirrorManifestError(
            f"Manifest {where} has {name} {raw!r} with no offset."
        )
    return value.astimezone(UTC)


def _format_timestamp(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError(f"Manifest timestamps must be timezone-aware, got {value!r}.")
    return value.astimezone(UTC).strftime(_TIMESTAMP_FORMAT)


# ---------------------------------------------------------------------------
# Manifest fetch
# ---------------------------------------------------------------------------

_manifest_cache: dict[str, tuple[float, MirrorManifest | None]] = {}
_manifest_lock = threading.Lock()


def fetch_manifest(*, base: str) -> MirrorManifest | None:
    """Return the manifest at *base*, or None if it cannot be read.

    A result, failure included, is reused for five minutes within the
    process. A failure is logged once per window as a warning and returned
    as None, and the function does not raise. The fetch uses the plain bulk-stream session,
    follows no redirects and reads at most 4 MiB.
    """
    with _manifest_lock:
        cached = _manifest_cache.get(base)
        if cached is not None and time.monotonic() - cached[0] < _MANIFEST_TTL_SECONDS:
            return cached[1]

        url = manifest_url(base=base)
        try:
            manifest = parse_manifest(_get_manifest_bytes(url))
        except (requests.RequestException, MirrorManifestError, OSError) as e:
            logger.warning(f"Bulk-file mirror manifest {url} is unavailable: {e}")
            manifest = None
        _manifest_cache[base] = (time.monotonic(), manifest)
        return manifest


def _get_manifest_bytes(url: str) -> bytes:
    with _get_bulk_stream_session().get(
        url,
        headers=DEFAULT_HEADERS,
        stream=True,
        timeout=_MANIFEST_TIMEOUT,
        allow_redirects=False,
    ) as r:
        if r.status_code != 200:
            raise OSError(f"HTTP {r.status_code}")
        body = bytearray()
        for chunk in r.iter_content(chunk_size=65536):
            body += chunk
            if len(body) > _MANIFEST_MAX_BYTES:
                raise OSError(f"manifest exceeds {_MANIFEST_MAX_BYTES} bytes")
        return bytes(body)


def _clear_manifest_cache() -> None:
    """Forget every cached manifest fetch (for tests)."""
    with _manifest_lock:
        _manifest_cache.clear()


# ---------------------------------------------------------------------------
# Replacement rule and verification
# ---------------------------------------------------------------------------


def mirror_may_replace(
    record: dict | None, m: MirrorFile, announced: str | None
) -> bool:
    """Return whether the mirror's copy *m* may stand in for the cached *record*.

    The mirror may replace the cache when there is no cached record, when
    OECD's announced version is the mirror's token, or when the cached
    record already holds the mirror's token. A record whose content hash
    equals the mirror's SHA-256 holds the same bytes, so the rule also holds
    and the caller skips the download. Otherwise the mirror may replace a
    record downloaded before the refresher last saw OECD serve *m*'s token
    (``m.checked_at``), since that copy is no newer than the mirror's.

    The last comparison sets this machine's clock against the refresher's.
    A local clock that runs ahead keeps the cached copy until a later
    refresh. A clock that runs behind by more than the gap between a refresh
    and an OECD republish can replace a newer cached copy with the mirror's
    older one. OECD republishes weeks apart and the refresher runs daily, so
    that needs a clock days slow.

    Args:
        record: The cache record for the file's URL, or None.
        m: The manifest entry for that URL.
        announced: The version OECD announced for the file, or None when the
            probe failed.

    Returns:
        True when the mirror's copy may be recorded in place of *record*.
    """
    if record is None or announced == m.token or holds_mirror_copy(record, m):
        return True
    downloaded_at = datetime.strptime(record["downloaded_at"], ISO_FORMAT)
    return downloaded_at < m.checked_at


def holds_mirror_copy(record: dict, m: MirrorFile) -> bool:
    """Return whether the cached *record*'s payload is the mirror's copy *m*.

    It is when the record carries *m*'s token, or when ``scan`` recorded
    *m*'s SHA-256 as its content hash.
    """
    return (
        record.get("version") == m.token
        or record.get("content_hash") == f"sha256:{m.sha256}"
    )


def mirror_token_stands(record: dict | None, m: MirrorFile) -> bool:
    """Return whether *m*'s token stands as the file's version after a challenged probe.

    With OECD's version probe challenged, the mirror's token is the best
    available version for the file unless the cache already holds a copy
    that `mirror_may_replace` keeps, or holds the mirror's own bytes under
    another token. In those two cases the cached copy's own token stands,
    so ``scan`` and ``release`` report the same token for the same file.
    """
    return record is None or (
        not holds_mirror_copy(record, m) and mirror_may_replace(record, m, None)
    )


def file_sha256(path: Path) -> str:
    """Return the SHA-256 of the file at *path* as lowercase hex."""
    with path.open("rb") as f:
        return hashlib.file_digest(f, "sha256").hexdigest()


def verify_mirror_file(path: Path, m: MirrorFile) -> None:
    """Check that the file at *path* has *m*'s size and SHA-256.

    Raises:
        MirrorIntegrityError: If the size or the digest differs.
    """
    size = path.stat().st_size
    if size != m.size:
        raise MirrorIntegrityError(
            f"Mirror object {m.object} is {size} bytes; the manifest says {m.size}."
        )
    digest = file_sha256(path)
    if digest != m.sha256:
        raise MirrorIntegrityError(
            f"Mirror object {m.object} has SHA-256 {digest}; the manifest says "
            f"{m.sha256}."
        )
