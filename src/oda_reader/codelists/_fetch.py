"""The ASPX three-step handshake against OECD's CodesList.aspx.

``fetch_codelists``, at the bottom of this module, is the public entry
point: ``parse_codelists`` composed with the handshake above.
"""

from __future__ import annotations

import random
import time
from collections.abc import MutableMapping, Sequence
from datetime import UTC, datetime
from html.parser import HTMLParser
from importlib.metadata import version as _pkg_version
from typing import Literal
from urllib.parse import urljoin, urlsplit

import requests

from oda_reader._http_primitives import API_RATE_LIMITER
from oda_reader.codelists._parse import parse_codelists
from oda_reader.codelists._types import (
    DEFAULT_URL,
    SUPPORTED_CODELIST_IDS,
    CodelistSnapshot,
)
from oda_reader.exceptions import (
    CodelistFetchError,
    CodelistShapeError,
    CodelistSourceError,
    CodelistValidationError,
)

# The handshake stages, matching CodelistFetchError/CodelistSourceError's
# `stage` Literal (src/oda_reader/exceptions.py) — not to be confused with
# CodelistShapeError's "hidden-field" | "envelope" | "row" vocabulary.
_Stage = Literal["page", "select", "download"]

# Read the same way src/oda_reader/_cache/config.py reads the package version.
_USER_AGENT = (
    f"oda-reader/{_pkg_version('oda_reader')} "
    "(+https://github.com/ONEcampaign/oda_reader)"
)

# HTTP statuses worth a fresh-handshake retry: transient server-side trouble,
# not a verdict on the request itself.
_RETRYABLE_STATUSES = frozenset({408, 425, 429})

# Same-origin redirects are followed, bounded to this many hops per step.
# The step-2 POST answers with a routine Post-Redirect-Get 302 to the result
# page; three hops is generous headroom above that one expected hop, with a
# hard stop so a same-origin loop can't spin forever.
_MAX_REDIRECT_HOPS = 3


class _HiddenInputParser(HTMLParser):
    """Collects every ``<input type="hidden">`` on a page into ``{name: value}``.

    A stdlib ``html.parser`` subclass that is order-independent and
    quote-style independent. A hidden input missing its ``value`` attribute
    yields ``""`` rather than being skipped.
    """

    def __init__(self) -> None:
        super().__init__()
        self.hidden_inputs: dict[str, str] = {}

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag != "input":
            return
        attr_map = dict(attrs)
        if (attr_map.get("type") or "").lower() != "hidden":
            return
        name = attr_map.get("name")
        if not name:
            return
        self.hidden_inputs[name] = attr_map.get("value") or ""


def _extract_hidden_inputs(html: str) -> dict[str, str]:
    """Parse *html* and return every hidden input found, as ``{name: value}``."""
    parser = _HiddenInputParser()
    parser.feed(html)
    return parser.hidden_inputs


def _require_hidden(
    hidden: dict[str, str], name: str, *, body: bytes, codelist_id: str
) -> None:
    """Raise CodelistShapeError if a required hidden token is absent or empty."""
    if not hidden.get(name):
        raise CodelistShapeError(
            stage="hidden-field",
            detail=f"required hidden input {name!r} is absent or empty",
            body=body,
            url=DEFAULT_URL,
            codelist_id=codelist_id,
        )


class _RetryableFailureError(Exception):
    """Internal signal: this attempt failed transiently and may be retried.

    Never escapes ``_fetch_codelist_bytes``: the retry loop there either
    starts a fresh handshake attempt or converts this into a
    ``CodelistFetchError`` once attempts are exhausted.
    """

    def __init__(self, *, stage: _Stage, status_code: int | None) -> None:
        self.stage = stage
        self.status_code = status_code
        super().__init__(f"retryable failure at {stage} stage (status={status_code})")


def _backoff_seconds(attempt: int) -> float:
    """Exponential backoff with jitter before retry attempt *attempt* (1-based)."""
    base = min(2 ** (attempt - 1), 8)
    return base + random.uniform(0, base)


def _same_origin(url: str, *, reference: str) -> bool:
    """True if *url* shares scheme and host with *reference*."""
    a, b = urlsplit(url), urlsplit(reference)
    return (a.scheme, a.netloc) == (b.scheme, b.netloc)


def _handshake_request(
    session: requests.Session,
    *,
    method: str,
    data: dict[str, str] | None,
    timeout: int,
    headers: dict[str, str],
    stage: _Stage,
) -> requests.Response:
    """One GET or POST to the OECD codelist app, same-origin redirects followed.

    ``API_RATE_LIMITER.wait()`` gates every call through here, so every
    request the handshake makes -- across all three steps, every followed
    redirect hop, and every retry attempt -- sits inside the same
    process-wide throttle as the rest of the package.

    Transport-level failures (timeout, connection reset, a mid-stream
    chunked-encoding break, a corrupt compressed body) become
    ``_RetryableFailureError`` here, so the retry loop only needs to catch one
    exception type. Everything else is returned as a ``requests.Response``
    for ``_check_response`` to classify by status code.

    **Redirects.** The step-2 POST answers with a routine Post-Redirect-Get
    302 to the result page. A same-origin redirect is followed here, bounded
    to ``_MAX_REDIRECT_HOPS`` hops. A cross-origin redirect, or a same-origin
    chain that exceeds the hop budget, is returned as-is, unfollowed, for
    ``_check_response`` to raise ``CodelistSourceError`` on. Following a
    POST's 301/302/303 switches the next hop to a bodyless GET; 307/308
    preserve the method and body.
    """
    url = DEFAULT_URL
    for hop in range(_MAX_REDIRECT_HOPS + 1):
        API_RATE_LIMITER.wait()
        try:
            if method == "GET":
                response = session.get(
                    url, timeout=timeout, headers=headers, allow_redirects=False
                )
            else:
                response = session.post(
                    url,
                    data=data,
                    timeout=timeout,
                    headers=headers,
                    allow_redirects=False,
                )
        except (
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
            # A server that resets a chunked response mid-stream, or returns
            # a corrupt compressed body, is a transient transport failure --
            # exactly what the retry loop is for -- not a verdict on the
            # request. Neither subclasses Timeout or ConnectionError, so
            # both are named explicitly. The set stops here deliberately:
            # catching requests.exceptions.RequestException wholesale would
            # also retry non-transient failures such as TooManyRedirects,
            # InvalidURL/MissingSchema (a malformed URL), or
            # UnrewindableBodyError -- bugs in the request itself that a
            # fresh handshake attempt cannot fix, so retrying would only
            # burn attempts and mask the real problem.
            requests.exceptions.ChunkedEncodingError,
            requests.exceptions.ContentDecodingError,
        ) as exc:
            raise _RetryableFailureError(stage=stage, status_code=None) from exc

        status = response.status_code
        if status is None or not (300 <= status < 400):
            return response
        location = response.headers.get("Location")
        if not location or hop == _MAX_REDIRECT_HOPS:
            # No Location to follow, or the hop budget is spent: hand back
            # the still-3xx response for _check_response to raise on.
            return response
        target = urljoin(url, location)
        if not _same_origin(target, reference=DEFAULT_URL):
            return response
        if method == "POST" and status in (301, 302, 303):
            method, data = "GET", None
        url = target
    return response  # pragma: no cover -- the loop above always returns first


def _check_response(
    response: requests.Response, *, stage: _Stage, codelist_id: str
) -> None:
    """Classify a handshake response's HTTP status.

    A response reaching here still carrying a 3xx status is one
    ``_handshake_request`` chose not to follow -- a cross-origin redirect, or
    a same-origin chain past the hop budget. Both mean we were bounced
    somewhere that is not our document, so it is reported as a
    ``CodelistSourceError`` carrying the ``Location``. A 2xx status returns
    normally — it is not proof the body is our document; the page-stage form
    check in ``_fetch_codelist_once`` covers that.
    """
    status = response.status_code
    # requests only leaves status_code as its None default when a request
    # object was never actually sent, which can't happen here: this response
    # came back from a session.get/post call that didn't raise.
    assert status is not None, "response.status_code is None after a live request"
    if 300 <= status < 400:
        raise CodelistSourceError(
            url=DEFAULT_URL,
            stage=stage,
            status_code=status,
            location=response.headers.get("Location"),
            body=response.content,
            codelist_id=codelist_id,
        )
    if status in _RETRYABLE_STATUSES or 500 <= status < 600:
        raise _RetryableFailureError(stage=stage, status_code=status)
    if status >= 400:
        raise CodelistSourceError(
            url=DEFAULT_URL,
            stage=stage,
            status_code=status,
            body=response.content,
            codelist_id=codelist_id,
        )


# Cblstatus$N indices requested on every fetch, always: status is not a
# public parameter, so there is exactly one request shape to maintain, fixture
# and test. The full index map is documented in _types.py:
#   Cblstatus$0 = Active
#   Cblstatus$1 = Future
#   Cblstatus$2 = Heading
#   Cblstatus$3 = Withdrawn
# The _statuses parameter below is a private test seam.
_ALL_STATUSES: tuple[str, ...] = ("0", "1", "2", "3")


def _deliberate_controls(
    *, codelist_id: str, standard: str, event_target: str, statuses: tuple[str, ...]
) -> dict[str, str]:
    """The WebForms controls set on every step-2/step-3 POST.

    Overlaid onto the echoed hidden inputs so these keys always carry our
    values, never the page's. In WebForms, a checkbox is present in the body
    when ticked and absent when not, so echoing what was found would silently
    submit the page's defaults.

    ``statuses`` selects which ``Cblstatus$N`` boxes are ticked.
    """
    controls = {
        "__EVENTTARGET": event_target,
        "DDl_codeslist": codelist_id,
        "DDL_CRSTOSSD": standard,
        "tb_search": "",
    }
    for idx in statuses:
        controls[f"Cblstatus${idx}"] = "on"
    return controls


def _fetch_codelist_once(
    *,
    codelist_id: str,
    standard: str,
    timeout: int,
    _capture: MutableMapping[str, str] | None = None,
    _statuses: tuple[str, ...] = _ALL_STATUSES,
) -> bytes:
    """One complete three-step ASPX handshake on a brand-new session.

    Never retries internally — ``_fetch_codelist_bytes`` is the retry loop,
    and each retry must be a complete fresh handshake. ``__VIEWSTATE`` and
    ``__EVENTVALIDATION`` are single-use server state, so re-posting a body
    whose tokens came from an already-consumed response is not a retry but
    a different and worse request.

    ``_capture``, when given a mutable mapping, is filled with this attempt's
    ``{"page_step1": <HTML>, "page_step2": <HTML>}``. Private, not part of
    the public surface.

    ``_statuses`` selects which ``Cblstatus$N`` boxes this attempt ticks,
    defaulting to all four. Private, not part of the public surface.
    """
    # A plain requests.Session(), never oda_reader._http_primitives'
    # requests_cache.CachedSession: a cached response to an intermediate POST
    # would replay a stale __VIEWSTATE, and the resulting failure would look
    # exactly like a shape change.
    session = requests.Session()
    headers = {"User-Agent": _USER_AGENT}

    # Step 1: GET the page for the initial VIEWSTATE tokens.
    resp1 = _handshake_request(
        session,
        method="GET",
        data=None,
        timeout=timeout,
        headers=headers,
        stage="page",
    )
    _check_response(resp1, stage="page", codelist_id=codelist_id)
    html1 = resp1.text
    # A 200 is not proof of life: maintenance pages and interstitials return
    # 200 too. Confirm this is actually CodesList.aspx before trusting its
    # hidden inputs.
    if "DDl_codeslist" not in html1:
        raise CodelistSourceError(
            url=DEFAULT_URL,
            stage="page",
            status_code=resp1.status_code,
            body=resp1.content,
            codelist_id=codelist_id,
        )
    hidden1 = _extract_hidden_inputs(html1)
    _require_hidden(hidden1, "__VIEWSTATE", body=resp1.content, codelist_id=codelist_id)
    _require_hidden(
        hidden1, "__EVENTVALIDATION", body=resp1.content, codelist_id=codelist_id
    )
    if _capture is not None:
        _capture["page_step1"] = html1

    # Step 2: POST to select the codelist (triggers a VIEWSTATE re-seed).
    # Echo every hidden input the page carried, then overlay only the
    # deliberate controls — a new required hidden field is already in the
    # dict and already flows through, without our needing to know its meaning.
    body2 = dict(hidden1)
    body2.update(
        _deliberate_controls(
            codelist_id=codelist_id,
            standard=standard,
            event_target="DDl_codeslist",
            statuses=_statuses,
        )
    )
    resp2 = _handshake_request(
        session,
        method="POST",
        data=body2,
        timeout=timeout,
        headers=headers,
        stage="select",
    )
    _check_response(resp2, stage="select", codelist_id=codelist_id)
    html2 = resp2.text
    hidden2 = _extract_hidden_inputs(html2)
    _require_hidden(hidden2, "__VIEWSTATE", body=resp2.content, codelist_id=codelist_id)
    _require_hidden(
        hidden2, "__EVENTVALIDATION", body=resp2.content, codelist_id=codelist_id
    )
    if _capture is not None:
        _capture["page_step2"] = html2

    # Step 3: POST to request the JSON download.
    body3 = dict(hidden2)
    body3.update(
        _deliberate_controls(
            codelist_id=codelist_id,
            standard=standard,
            event_target="",
            statuses=_statuses,
        )
    )
    body3["b_json"] = "JSON"
    resp3 = _handshake_request(
        session,
        method="POST",
        data=body3,
        timeout=timeout,
        headers=headers,
        stage="download",
    )
    _check_response(resp3, stage="download", codelist_id=codelist_id)
    return resp3.content


def _fetch_codelist_bytes(
    codelist_id: str,
    *,
    standard: str = "0",
    timeout: int = 30,
    retries: int = 2,
    _capture: MutableMapping[str, str] | None = None,
    _statuses: tuple[str, ...] = _ALL_STATUSES,
) -> bytes:
    """Fetch a codelist from the OECD codelist app via the ASPX three-step handshake.

    Each attempt is a complete fresh-session handshake (GET the page, POST
    to select the codelist, POST to download the JSON) on a brand-new
    ``requests.Session``. Retries only transport failures (timeout,
    connection reset, a mid-stream chunked-encoding break, a corrupt
    compressed body) and HTTP 5xx/429/408/425, with exponential backoff and
    jitter between attempts. A 4xx, a refused redirect, or a shape failure
    is never retried.

    Args:
        codelist_id: OECD codelist ID (e.g. "5" for providers, "13" for recipients).
        standard: DDL_CRSTOSSD value (default "0").
        timeout: Per-request timeout in seconds.
        retries: Additional attempts after the first, so total attempts is
            ``retries + 1``.
        _capture: Private fixture-capture hook. When given a mutable mapping,
            it is filled with the successful attempt's ``{"page_step1": <html>,
            "page_step2": <html>}``. Not part of the public surface.
        _statuses: Private status-selection seam. Which ``Cblstatus$N`` boxes
            ("0".."3") to tick, defaulting to ``_ALL_STATUSES``. Not part of
            the public surface.

    Returns:
        Raw JSON bytes from the OECD server.

    Raises:
        CodelistFetchError: Transport failures or 5xx/429/408/425 persisted
            through every attempt.
        CodelistSourceError: A 4xx, an unfollowed redirect (cross-origin, or
            a same-origin chain past the hop budget), or a 200 response that
            is not the CodesList.aspx page.
        CodelistShapeError: A required hidden token was absent or empty.
    """
    for attempt in range(1, retries + 2):
        try:
            return _fetch_codelist_once(
                codelist_id=codelist_id,
                standard=standard,
                timeout=timeout,
                _capture=_capture,
                _statuses=_statuses,
            )
        except _RetryableFailureError as exc:
            if attempt == retries + 1:
                raise CodelistFetchError(
                    url=DEFAULT_URL,
                    stage=exc.stage,
                    attempts=attempt,
                    status_code=exc.status_code,
                    codelist_id=codelist_id,
                ) from exc
            time.sleep(_backoff_seconds(attempt))
    raise AssertionError("unreachable: loop always returns or raises")


def _validate_codelist_ids(codelist_ids: Sequence[str]) -> None:
    """Validate *codelist_ids* before ``fetch_codelists`` issues any request.

    Security review: argument validation must happen before the network is
    touched, not after. Raises ``CodelistValidationError`` naming the
    offending value -- empty, an id outside ``SUPPORTED_CODELIST_IDS``, or a
    duplicate.
    """
    if not codelist_ids:
        raise CodelistValidationError(detail="codelist_ids must not be empty")
    seen: set[str] = set()
    for codelist_id in codelist_ids:
        if codelist_id not in SUPPORTED_CODELIST_IDS:
            raise CodelistValidationError(
                detail=(
                    f"unsupported codelist_id {codelist_id!r}; supported: "
                    f"{SUPPORTED_CODELIST_IDS}"
                ),
                codelist_id=codelist_id,
            )
        if codelist_id in seen:
            raise CodelistValidationError(
                detail=f"duplicate codelist_id {codelist_id!r}",
                codelist_id=codelist_id,
            )
        seen.add(codelist_id)


def fetch_codelists(
    *,
    codelist_ids: Sequence[str] = ("5", "13"),
    timeout: int = 30,
    retries: int = 2,
) -> CodelistSnapshot:
    """Fetch and parse OECD DAC codelists via the live ASPX handshake.

    Named ``fetch_*`` rather than ``download_*`` like the rest of the
    package's readers on purpose: ``download_*`` functions return a bare
    DataFrame, while this returns a ``CodelistSnapshot`` -- the frame plus
    the raw bytes, fetch time and source URL behind it. The different verb
    signals the different return contract.

    ``parse_codelists`` is composed with the handshake above. Every guarantee
    documented on ``CodelistSnapshot`` and ``parse_codelists`` holds here too.

    Requests all four ``Cblstatus$N`` boxes on every call and filters in
    Python; status is not a parameter here.

    Args:
        codelist_ids: Which codelists to fetch, in request order. Each must
            be one of the supported ids (``{"5", "13"}``); duplicates are
            rejected.
        timeout: Per-request timeout in seconds, applied to every request
            of every codelist's handshake.
        retries: Additional attempts after the first, per codelist, so
            total attempts per codelist is ``retries + 1``.

    Returns:
        A ``CodelistSnapshot`` covering every id in *codelist_ids*.

    Raises:
        CodelistValidationError: *codelist_ids* is empty, names an
            unsupported codelist, or contains a duplicate -- raised before
            any request is made. Also raised if a codelist's payload
            parses to zero rows.
        CodelistFetchError: Transport failures or 5xx/429/408/425 persisted
            through every attempt, for any one codelist.
        CodelistSourceError: A 4xx, an unfollowed redirect (cross-origin,
            or a same-origin chain past the hop budget), or a 200 response
            that is not the CodesList.aspx page, for any one codelist.
        CodelistShapeError: A required hidden token was absent or empty, or
            the downloaded payload's shape did not match what ``parse_codelists``
            expects.
    """
    _validate_codelist_ids(codelist_ids)
    raw: dict[str, bytes] = {
        codelist_id: _fetch_codelist_bytes(
            codelist_id, timeout=timeout, retries=retries
        )
        for codelist_id in codelist_ids
    }
    return parse_codelists(
        raw=raw, fetched_at=datetime.now(UTC), source_url=DEFAULT_URL
    )
