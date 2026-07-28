"""Offline tests for the hardened OECD codelist ASPX handshake, and for
`fetch_codelists` — the handshake composed with `parse_codelists`.

All tests are @pytest.mark.unit, run against a stubbed requests.Session, and
require no network access. The two tests over page_step1.html/page_step2.html
skip automatically until those fixtures exist.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import requests
from _helpers import envelope as _envelope

import oda_reader.codelists._fetch as mod
from oda_reader.exceptions import (
    CodelistFetchError,
    CodelistShapeError,
    CodelistSourceError,
    CodelistValidationError,
)

_FIXTURES = Path(__file__).parent.parent.parent / "fixtures" / "oecd"


def _load_fixture(filename: str) -> str:
    path = _FIXTURES / filename
    if not path.exists():
        pytest.skip(
            f"Fixture {filename} not yet captured — run --capture-fixtures first"
        )
    return path.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Inline page builder (no fixture needed) — for scenarios with no dedicated
# hand-built fixture, e.g. "hidden field present but empty".
# ---------------------------------------------------------------------------


def _page_html(
    *,
    viewstate: str | None = "VS_TOKEN==",
    generator: str | None = "VSGEN_TOKEN",
    eventvalidation: str | None = "EV_TOKEN==",
) -> str:
    """A minimal CodesList.aspx page with configurable hidden tokens.

    A token set to None is omitted entirely (the "absent" negative); an
    empty string keeps the input but with an empty value (the "empty"
    negative). Always carries the DDl_codeslist control, so callers that
    don't care about the page-stage form check need not think about it.
    """
    hidden = []
    if viewstate is not None:
        hidden.append(f'<input type="hidden" name="__VIEWSTATE" value="{viewstate}" />')
    if generator is not None:
        hidden.append(
            f'<input type="hidden" name="__VIEWSTATEGENERATOR" value="{generator}" />'
        )
    if eventvalidation is not None:
        hidden.append(
            f'<input type="hidden" name="__EVENTVALIDATION" value="{eventvalidation}" />'
        )
    return (
        '<html><body><form id="form1" method="post" action="./CodesList.aspx">'
        + "".join(hidden)
        + '<select name="DDl_codeslist" id="DDl_codeslist">'
        + '<option value="5">Providers</option></select>'
        + "</form></body></html>"
    )


_ROW_5 = {
    "status": "Active",
    "code": "1",
    "name": {"narrative": ["Austria", {"xml:lang": "fr", "#text": "Autriche"}]},
    "type": "DAC member",
    "iso-alpha-3-code": "AUT",
    "dotstatcode": "AUT",
    "crs": "1",
    "tossd": "1",
}
_ROW_13 = {
    "status": "active",
    "code": "231",
    "name": {"narrative": ["Austria", {"xml:lang": "fr", "#text": "Autriche"}]},
    "iso-alpha-3-code": "AUT",
    "dotstatcode": "AUT",
    "crs": "1",
    "tossd": "1",
}


# ---------------------------------------------------------------------------
# requests.Session stand-in
# ---------------------------------------------------------------------------


class _StubResponse:
    """A requests.Response stand-in carrying only what the handshake reads."""

    def __init__(
        self,
        *,
        status_code: int = 200,
        text: str = "",
        content: bytes | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.status_code = status_code
        self.text = text
        self.content = content if content is not None else text.encode("utf-8")
        self.headers = headers or {}


class _ScriptedSession:
    """Stand-in for one requests.Session, driven by a queue of scripted steps.

    Each step is a ``_StubResponse`` (returned) or an ``Exception`` instance
    (raised), consumed in order across get()/post() calls. Every call is
    recorded so a test can assert exactly which requests were made.
    """

    def __init__(self, steps: list) -> None:
        self._steps = list(steps)
        self.calls: list[tuple[str, dict[str, str] | None]] = []

    def _next(self, method: str, data: dict[str, str] | None) -> _StubResponse:
        self.calls.append((method, data))
        if not self._steps:
            raise AssertionError("scripted session ran out of steps")
        step = self._steps.pop(0)
        if isinstance(step, Exception):
            raise step
        return step

    def get(self, url: str, *, timeout: int, headers: dict, allow_redirects: bool):
        assert allow_redirects is False, "every handshake request must refuse redirects"
        return self._next("GET", None)

    def post(
        self,
        url: str,
        *,
        data: dict[str, str],
        timeout: int,
        headers: dict,
        allow_redirects: bool,
    ):
        assert allow_redirects is False, "every handshake request must refuse redirects"
        return self._next("POST", data)


class _SessionFactory:
    """Monkeypatch target for ``requests.Session``.

    Hands out one scripted session per call, in order, so each retry attempt
    gets its own fresh session — a retry is always a new handshake, never a
    replayed POST.
    """

    def __init__(self, sessions: list[list]) -> None:
        self._sessions = list(sessions)
        self.created: list[_ScriptedSession] = []

    def __call__(self) -> _ScriptedSession:
        steps = self._sessions.pop(0)
        session = _ScriptedSession(steps)
        self.created.append(session)
        return session


def _install(monkeypatch: pytest.MonkeyPatch, sessions: list[list]) -> _SessionFactory:
    """Patch requests.Session and silence real sleeps for one test."""
    factory = _SessionFactory(sessions)
    monkeypatch.setattr(mod.requests, "Session", factory)
    monkeypatch.setattr(mod.time, "sleep", lambda _seconds: None)
    return factory


# ---------------------------------------------------------------------------
# Hidden-input extraction
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_hidden_input_extraction_survives_reordered_attrs_and_single_quotes() -> None:
    """html.parser must extract hidden-field tokens regardless of attribute
    order or quote style."""
    html = _load_fixture("page_bad_attr_order.html")
    hidden = mod._extract_hidden_inputs(html)
    assert hidden.get("__VIEWSTATE") == "VS_BADORDER_TOKEN=="
    assert hidden.get("__VIEWSTATEGENERATOR") == "VSGEN_BADORDER"
    assert hidden.get("__EVENTVALIDATION") == "EV_BADORDER_TOKEN=="


@pytest.mark.unit
def test_hidden_input_extraction_over_real_markup() -> None:
    """Skips cleanly until the fixtures are captured."""
    for filename in ("page_step1.html", "page_step2.html"):
        html = _load_fixture(filename)
        hidden = mod._extract_hidden_inputs(html)
        assert hidden.get("__VIEWSTATE"), filename
        assert hidden.get("__VIEWSTATEGENERATOR"), filename
        assert hidden.get("__EVENTVALIDATION"), filename


# ---------------------------------------------------------------------------
# Echo every hidden input, overlay only the deliberate controls
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_echo_all_hidden_inputs_overlay_wins(monkeypatch: pytest.MonkeyPatch) -> None:
    """page_extra_hidden.html carries an unseen __NEWTOKEN plus decoy hidden
    values for the controls we overlay deliberately. __NEWTOKEN must survive
    verbatim; DDl_codeslist, the Cblstatus$N boxes and b_json must carry our
    values, never the page's."""
    page = _load_fixture("page_extra_hidden.html")
    factory = _install(
        monkeypatch,
        [
            [
                _StubResponse(status_code=200, text=page),  # step 1
                _StubResponse(status_code=200, text=page),  # step 2 (re-seed)
                _StubResponse(status_code=200, content=b"raw-json-bytes"),  # step 3
            ]
        ],
    )
    result = mod._fetch_codelist_bytes("5")
    assert result == b"raw-json-bytes"

    session = factory.created[0]
    assert len(session.calls) == 3
    _, body2 = session.calls[1]
    _, body3 = session.calls[2]
    assert body2 is not None and body3 is not None

    # The unseen field is echoed verbatim in both POSTs.
    assert body2["__NEWTOKEN"] == "unseen-value-12345"
    assert body3["__NEWTOKEN"] == "unseen-value-12345"

    # The deliberate controls carry OUR values, not the page's decoys
    # ("999", "off", "off", "").
    assert body2["DDl_codeslist"] == "5"
    assert body2["Cblstatus$0"] == "on"
    assert body2["Cblstatus$2"] == "on"
    assert body3["DDl_codeslist"] == "5"
    assert body3["Cblstatus$0"] == "on"
    assert body3["Cblstatus$2"] == "on"
    assert body3["b_json"] == "JSON"


# ---------------------------------------------------------------------------
# Hidden-input negatives
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_missing_viewstate_raises_shape_error(monkeypatch: pytest.MonkeyPatch) -> None:
    html = _load_fixture("page_no_viewstate.html")
    factory = _install(monkeypatch, [[_StubResponse(status_code=200, text=html)]])
    with pytest.raises(CodelistShapeError) as excinfo:
        mod._fetch_codelist_bytes("5")
    assert excinfo.value.stage == "hidden-field"
    assert "__VIEWSTATE" in excinfo.value.detail
    assert len(factory.created) == 1


@pytest.mark.unit
def test_empty_viewstate_raises_shape_error(monkeypatch: pytest.MonkeyPatch) -> None:
    html = _page_html(viewstate="")
    factory = _install(monkeypatch, [[_StubResponse(status_code=200, text=html)]])
    with pytest.raises(CodelistShapeError) as excinfo:
        mod._fetch_codelist_bytes("5")
    assert excinfo.value.stage == "hidden-field"
    assert "__VIEWSTATE" in excinfo.value.detail
    assert len(factory.created) == 1


# ---------------------------------------------------------------------------
# Redirects: same-origin followed, cross-origin refused, hop budget bounded
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.parametrize("stage_index", [0, 1, 2], ids=["page", "select", "download"])
def test_cross_origin_redirect_raises_and_is_not_retried(
    monkeypatch: pytest.MonkeyPatch, stage_index: int
) -> None:
    """A redirect to a different scheme/host is refused outright, blocking
    threats like consent pages, block pages, or relocated hosts. Same-origin
    redirects are still followed."""
    good_page = _page_html()
    redirect = _StubResponse(
        status_code=302, headers={"Location": "https://example.org/blocked"}
    )
    steps = [
        _StubResponse(status_code=200, text=good_page),
        _StubResponse(status_code=200, text=good_page),
        _StubResponse(status_code=200, content=b"{}"),
    ]
    steps[stage_index] = redirect
    factory = _install(monkeypatch, [steps])

    with pytest.raises(CodelistSourceError) as excinfo:
        mod._fetch_codelist_bytes("5")

    assert excinfo.value.location == "https://example.org/blocked"
    session = factory.created[0]
    # No request was made past the refused redirect.
    assert len(session.calls) == stage_index + 1
    # No retry occurred.
    assert len(factory.created) == 1


@pytest.mark.unit
def test_same_origin_redirect_is_followed(monkeypatch: pytest.MonkeyPatch) -> None:
    """The step-2 POST answers with a Post-Redirect-Get 302 to the result
    page on the same host. It must be followed as a bodyless GET (since a
    POST's 301/302/303 switches method), and the handshake proceeds with
    re-seeded tokens from the redirect response."""
    step1_page = _page_html()
    redirected_page = _page_html(
        viewstate="VS_AFTER_REDIRECT", eventvalidation="EV_AFTER_REDIRECT"
    )
    steps = [
        _StubResponse(status_code=200, text=step1_page),  # step 1
        _StubResponse(
            status_code=302, headers={"Location": "/Codeslist.aspx"}
        ),  # step 2 POST -> same-origin Post-Redirect-Get
        _StubResponse(status_code=200, text=redirected_page),  # followed, as GET
        _StubResponse(status_code=200, content=b"raw-json-bytes"),  # step 3
    ]
    factory = _install(monkeypatch, [steps])

    result = mod._fetch_codelist_bytes("5")
    assert result == b"raw-json-bytes"

    session = factory.created[0]
    assert len(session.calls) == 4
    assert [method for method, _ in session.calls] == ["GET", "POST", "GET", "POST"]
    # The follow drops the POST body — it becomes a plain GET.
    assert session.calls[2][1] is None
    # Step 3 carries the tokens re-seeded by the followed redirect's page,
    # not the pre-redirect step-1 tokens.
    _, body3 = session.calls[3]
    assert body3["__VIEWSTATE"] == "VS_AFTER_REDIRECT"


@pytest.mark.unit
def test_same_origin_redirect_loop_exceeding_hop_budget_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A same-origin redirect chain longer than the hop budget must raise
    rather than spin forever."""
    origin = "https://development-finance-codelists.oecd.org"

    def _redirect(location: str) -> _StubResponse:
        return _StubResponse(status_code=302, headers={"Location": location})

    # One more hop than the budget allows, so the last is never followed.
    hop_urls = [f"{origin}/hop{n}" for n in range(mod._MAX_REDIRECT_HOPS + 1)]
    steps = [_redirect(url) for url in hop_urls]
    factory = _install(monkeypatch, [steps])

    with pytest.raises(CodelistSourceError) as excinfo:
        mod._fetch_codelist_bytes("5")

    assert excinfo.value.location == hop_urls[-1]
    session = factory.created[0]
    # The initial request plus every hop up to the budget, then stop.
    assert len(session.calls) == mod._MAX_REDIRECT_HOPS + 1
    # No retry occurred.
    assert len(factory.created) == 1


# ---------------------------------------------------------------------------
# Not-our-document
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_maintenance_page_raises_source_error_at_page_stage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A 200 is not proof of life: a maintenance page must be caught here,
    not surface as a downstream token error."""
    html = _load_fixture("page_maintenance.html")
    factory = _install(monkeypatch, [[_StubResponse(status_code=200, text=html)]])
    with pytest.raises(CodelistSourceError) as excinfo:
        mod._fetch_codelist_bytes("5")
    assert excinfo.value.stage == "page"
    assert excinfo.value.body is not None
    assert len(factory.created) == 1


# ---------------------------------------------------------------------------
# Retry behaviour
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_transient_timeout_is_retried_once(monkeypatch: pytest.MonkeyPatch) -> None:
    good_page1 = _page_html(viewstate="VS_A", eventvalidation="EV_A")
    good_page2 = _page_html(viewstate="VS_A2", eventvalidation="EV_A2")
    factory = _install(
        monkeypatch,
        [
            [requests.exceptions.Timeout("boom")],
            [
                _StubResponse(status_code=200, text=good_page1),
                _StubResponse(status_code=200, text=good_page2),
                _StubResponse(status_code=200, content=b"payload-bytes"),
            ],
        ],
    )
    result = mod._fetch_codelist_bytes("5")
    assert result == b"payload-bytes"
    assert len(factory.created) == 2  # one retry


@pytest.mark.unit
def test_404_raises_source_error_with_zero_retries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = _install(
        monkeypatch, [[_StubResponse(status_code=404, content=b"not found")]]
    )
    with pytest.raises(CodelistSourceError) as excinfo:
        mod._fetch_codelist_bytes("5")
    assert excinfo.value.status_code == 404
    assert len(factory.created) == 1


@pytest.mark.unit
def test_chunked_encoding_error_is_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    """A mid-stream reset while reading a chunked response body is a transient
    transport failure, not a verdict on the request -- it must go through
    the same retry loop as a Timeout or ConnectionError, not escape as a raw
    requests exception."""
    good_page1 = _page_html(viewstate="VS_A", eventvalidation="EV_A")
    good_page2 = _page_html(viewstate="VS_A2", eventvalidation="EV_A2")
    factory = _install(
        monkeypatch,
        [
            [requests.exceptions.ChunkedEncodingError("connection broken")],
            [
                _StubResponse(status_code=200, text=good_page1),
                _StubResponse(status_code=200, text=good_page2),
                _StubResponse(status_code=200, content=b"payload-bytes"),
            ],
        ],
    )
    result = mod._fetch_codelist_bytes("5")
    assert result == b"payload-bytes"
    assert len(factory.created) == 2  # one retry


@pytest.mark.unit
def test_content_decoding_error_is_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    """A corrupt compressed response body is a transient transport failure
    too, and must be retried the same way."""
    good_page1 = _page_html(viewstate="VS_A", eventvalidation="EV_A")
    good_page2 = _page_html(viewstate="VS_A2", eventvalidation="EV_A2")
    factory = _install(
        monkeypatch,
        [
            [requests.exceptions.ContentDecodingError("bad gzip")],
            [
                _StubResponse(status_code=200, text=good_page1),
                _StubResponse(status_code=200, text=good_page2),
                _StubResponse(status_code=200, content=b"payload-bytes"),
            ],
        ],
    )
    result = mod._fetch_codelist_bytes("5")
    assert result == b"payload-bytes"
    assert len(factory.created) == 2  # one retry


@pytest.mark.unit
def test_persistent_chunked_encoding_error_raises_fetch_error_not_raw_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exhausted retries on a stream failure must surface as the documented
    CodelistFetchError, not the raw requests exception escaping the retry
    loop entirely."""
    factory = _install(
        monkeypatch,
        [
            [requests.exceptions.ChunkedEncodingError("connection broken")],
            [requests.exceptions.ChunkedEncodingError("connection broken")],
            [requests.exceptions.ChunkedEncodingError("connection broken")],
        ],
    )
    with pytest.raises(CodelistFetchError) as excinfo:
        mod._fetch_codelist_bytes("5")
    assert excinfo.value.attempts == 3
    assert len(factory.created) == 3


@pytest.mark.unit
def test_persistent_503_raises_fetch_error_after_three_attempts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = _install(
        monkeypatch,
        [
            [_StubResponse(status_code=503)],
            [_StubResponse(status_code=503)],
            [_StubResponse(status_code=503)],
        ],
    )
    with pytest.raises(CodelistFetchError) as excinfo:
        mod._fetch_codelist_bytes("5")
    assert excinfo.value.attempts == 3
    assert len(factory.created) == 3


@pytest.mark.unit
def test_retry_restarts_the_full_handshake_with_fresh_tokens(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A retry is a complete fresh handshake, never a replayed POST. The
    second attempt must issue its own step-1 GET, and its step-3 POST must
    carry its own re-seeded tokens — never a token from the aborted first
    attempt."""
    session2_step1 = _page_html(viewstate="SESSION2_STEP1_VS", eventvalidation="EV_1")
    session2_step2 = _page_html(viewstate="SESSION2_STEP2_VS", eventvalidation="EV_2")
    factory = _install(
        monkeypatch,
        [
            [requests.exceptions.ConnectionError("reset")],  # attempt 1: dies on GET
            [
                _StubResponse(status_code=200, text=session2_step1),
                _StubResponse(status_code=200, text=session2_step2),
                _StubResponse(status_code=200, content=b"final-bytes"),
            ],
        ],
    )
    result = mod._fetch_codelist_bytes("5")
    assert result == b"final-bytes"

    assert len(factory.created) == 2
    session1, session2 = factory.created
    assert len(session1.calls) == 1  # only the aborted GET
    assert len(session2.calls) == 3  # a complete fresh handshake

    method, _ = session2.calls[0]
    assert method == "GET"

    _, body3 = session2.calls[2]
    assert body3["__VIEWSTATE"] == "SESSION2_STEP2_VS"


# ---------------------------------------------------------------------------
# All four statuses are requested by default
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_fetch_codelist_bytes_default_requests_all_four_statuses(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`_fetch_codelist_bytes` ticks Cblstatus$0 through $3 with no `_statuses` override."""
    page = _page_html()
    factory = _install(
        monkeypatch,
        [
            [
                _StubResponse(status_code=200, text=page),
                _StubResponse(status_code=200, text=page),
                _StubResponse(status_code=200, content=b"raw-json-bytes"),
            ]
        ],
    )
    mod._fetch_codelist_bytes("5")
    _, body2 = factory.created[0].calls[1]
    for idx in ("0", "1", "2", "3"):
        assert body2[f"Cblstatus${idx}"] == "on"


# ---------------------------------------------------------------------------
# fetch_codelists — the handshake composed with parse_codelists
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_fetch_codelists_rejects_empty_codelist_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = _install(monkeypatch, [])
    with pytest.raises(CodelistValidationError) as excinfo:
        mod.fetch_codelists(codelist_ids=())
    assert "empty" in excinfo.value.detail
    assert len(factory.created) == 0, "validation must happen before any request"


@pytest.mark.unit
def test_fetch_codelists_rejects_unsupported_codelist_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = _install(monkeypatch, [])
    with pytest.raises(CodelistValidationError) as excinfo:
        mod.fetch_codelists(codelist_ids=("5", "999"))
    assert "999" in excinfo.value.detail
    assert excinfo.value.codelist_id == "999"
    assert len(factory.created) == 0, "validation must happen before any request"


@pytest.mark.unit
def test_fetch_codelists_rejects_duplicate_codelist_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = _install(monkeypatch, [])
    with pytest.raises(CodelistValidationError) as excinfo:
        mod.fetch_codelists(codelist_ids=("5", "5"))
    assert "5" in excinfo.value.detail
    assert len(factory.created) == 0, "validation must happen before any request"


@pytest.mark.unit
def test_fetch_codelists_rejects_bare_str_codelist_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """codelist_ids="13" must raise, not silently iterate into "1" and "3"
    and fetch two unrelated, individually-valid codelists -- str IS a
    Sequence[str], so nothing short of an explicit guard catches this."""
    factory = _install(monkeypatch, [])
    with pytest.raises(CodelistValidationError) as excinfo:
        mod.fetch_codelists(codelist_ids="13")
    assert "bare str" in excinfo.value.detail
    assert len(factory.created) == 0, "validation must happen before any request"


@pytest.mark.unit
def test_fetch_codelists_rejects_non_str_codelist_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = _install(monkeypatch, [])
    with pytest.raises(CodelistValidationError) as excinfo:
        mod.fetch_codelists(codelist_ids=(5,))  # type: ignore[arg-type]
    assert "must be a str" in excinfo.value.detail
    assert len(factory.created) == 0, "validation must happen before any request"


@pytest.mark.unit
def test_fetch_codelists_composes_handshake_and_parse(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`fetch_codelists` composes `parse_codelists` with the handshake, not
    a distinct implementation. One fresh handshake per requested codelist id."""
    page = _page_html()
    payload_5 = _envelope("Providers", _ROW_5)
    payload_13 = _envelope("Recipients", _ROW_13)
    factory = _install(
        monkeypatch,
        [
            [
                _StubResponse(status_code=200, text=page),
                _StubResponse(status_code=200, text=page),
                _StubResponse(status_code=200, content=payload_5),
            ],
            [
                _StubResponse(status_code=200, text=page),
                _StubResponse(status_code=200, text=page),
                _StubResponse(status_code=200, content=payload_13),
            ],
        ],
    )

    snapshot = mod.fetch_codelists(codelist_ids=("5", "13"))

    assert snapshot.codelist_ids == ("5", "13")
    assert dict(snapshot.raw) == {"5": payload_5, "13": payload_13}
    assert set(snapshot.frame["codelist_id"]) == {"5", "13"}
    assert len(factory.created) == 2  # one complete handshake per codelist


@pytest.mark.unit
def test_fetch_codelists_exception_carries_the_failing_codelist_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failure on the second of two requested codelists must name that codelist, not the
    first one or none at all — every exception carries codelist_id where known."""
    page = _page_html()
    payload_5 = _envelope("Providers", _ROW_5)
    factory = _install(
        monkeypatch,
        [
            [
                _StubResponse(status_code=200, text=page),
                _StubResponse(status_code=200, text=page),
                _StubResponse(status_code=200, content=payload_5),
            ],
            [_StubResponse(status_code=404, content=b"not found")],
        ],
    )

    with pytest.raises(CodelistSourceError) as excinfo:
        mod.fetch_codelists(codelist_ids=("5", "13"))

    assert excinfo.value.codelist_id == "13"
    assert len(factory.created) == 2
