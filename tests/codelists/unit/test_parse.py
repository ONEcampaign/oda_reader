"""Offline tests for `parse_codelists`'s envelope and row validation.

All tests are @pytest.mark.unit, construct payloads inline (the `_envelope`
helper mirrors `tests/schemas/unit/test_refresh_dac_codelists.py`'s), and
require no network access — the last test in this file asserts that
property directly, the same way `test_frozen_boundary.py` does for
`import oda_reader`.
"""

from __future__ import annotations

import json
import subprocess
import sys
from datetime import UTC, datetime, timedelta, timezone

import pytest
from _helpers import envelope as _envelope
from _helpers import load_fixture as _load_fixture

from oda_reader.codelists import parse_codelists
from oda_reader.codelists._parse import _select_english_label
from oda_reader.codelists._types import DEFAULT_URL
from oda_reader.exceptions import CodelistShapeError, CodelistValidationError

_FETCHED_AT = datetime(2026, 1, 1, tzinfo=UTC)


_ROW_1 = {
    "status": "Active",
    "code": "1",
    "name": {"narrative": ["Austria", {"xml:lang": "fr", "#text": "Autriche"}]},
    "type": "DAC member",
    "iso-alpha-3-code": "AUT",
    "dotstatcode": "AUT",
    "crs": "1",
    "tossd": "1",
}
_ROW_2 = {
    "status": "active",
    "code": "2",
    "name": {"narrative": ["Belgium", {"xml:lang": "fr", "#text": "Belgique"}]},
    "type": "DAC member",
    "iso-alpha-3-code": "BEL",
    "dotstatcode": "BEL",
    "crs": "1",
    "tossd": "1",
    "activation-date": "1996-01-01",
}


# ---------------------------------------------------------------------------
# _select_english_label: both xml:lang spellings, unprefixed and underscore
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_select_english_label_recognises_unprefixed_xml_lang() -> None:
    """The top-level `name` field's shape: the French entry carries the
    unprefixed `xml:lang` attribute."""
    name_field = {"narrative": ["Austria", {"xml:lang": "fr", "#text": "Autriche"}]}
    assert _select_english_label(name_field) == "Austria"


@pytest.mark.unit
def test_select_english_label_recognises_underscore_prefixed_xml_lang() -> None:
    """The nested `Agencytype.name` field's shape: OECD tags the French entry
    with `_xml:lang`, underscore-prefixed, unlike the top-level `name`'s
    unprefixed form. Without recognising both spellings, this entry wouldn't
    be recognised as tagged at all -- picking English would only work by
    accident of it sitting first in the array."""
    name_field = {
        "narrative": [
            "Main Aid Agencies (in terms of budget)",
            {"_xml:lang": "fr", "#text": "Agences d'aide principales"},
        ]
    }
    assert _select_english_label(name_field) == "Main Aid Agencies (in terms of budget)"


@pytest.mark.unit
def test_select_english_label_finds_english_even_when_french_is_first() -> None:
    """The case that would silently return French without recognising both
    `xml:lang` spellings: with an unrecognised language tag, the selector
    falls back to `narrative[0]` unconditionally -- so a French-first array
    would return French. Recognising `_xml:lang` means the French entry is
    correctly skipped regardless of its position, and the plain (untagged)
    English entry is picked instead."""
    name_field = {
        "narrative": [
            {"_xml:lang": "fr", "#text": "Agences francophones"},
            "French-Language Agencies",
        ]
    }
    assert _select_english_label(name_field) == "French-Language Agencies"


@pytest.mark.unit
def test_select_english_label_returns_none_for_non_dict_input() -> None:
    assert _select_english_label(None) is None
    assert _select_english_label("not-a-dict") is None


# ---------------------------------------------------------------------------
# _select_english_label: an explicitly `en`-tagged entry outranks the
# untagged-means-English heuristic. Latent today -- no row's name.narrative
# in any fixture tags English -- but the heuristic breaks the moment one
# does, so these pin the priority order before that ever happens live.
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_select_english_label_prefers_explicit_en_tag_unprefixed() -> None:
    """An `xml:lang: "en"` entry wins over an untagged sibling, even though
    the untagged-first heuristic would otherwise return the untagged one."""
    name_field = {
        "narrative": [
            {"xml:lang": "en", "#text": "English"},
            "Francais",
        ]
    }
    assert _select_english_label(name_field) == "English"


@pytest.mark.unit
def test_select_english_label_prefers_explicit_en_tag_underscore_prefixed() -> None:
    """Same as the unprefixed case, for the underscore-prefixed spelling the
    nested `Agencytype.name` field uses."""
    name_field = {
        "narrative": [
            {"_xml:lang": "en", "#text": "English"},
            "Francais",
        ]
    }
    assert _select_english_label(name_field) == "English"


@pytest.mark.unit
def test_select_english_label_finds_explicit_en_tag_regardless_of_position() -> None:
    """The explicit-English entry wins even when it sits second, after an
    untagged entry the old heuristic would have already returned."""
    name_field = {
        "narrative": [
            "Francais",
            {"_xml:lang": "en", "#text": "English"},
        ]
    }
    assert _select_english_label(name_field) == "English"


# ---------------------------------------------------------------------------
# Envelope failures
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_not_json_raises_shape_error_at_envelope_stage() -> None:
    with pytest.raises(CodelistShapeError) as excinfo:
        parse_codelists(raw={"5": b"not json at all"}, fetched_at=_FETCHED_AT)
    assert excinfo.value.stage == "envelope"
    assert excinfo.value.body == b"not json at all"
    assert excinfo.value.codelist_id == "5"
    assert excinfo.value.url == DEFAULT_URL


@pytest.mark.unit
def test_missing_codelists_key_raises_shape_error_at_envelope_stage() -> None:
    body = json.dumps({"something-else": True}).encode()
    with pytest.raises(CodelistShapeError) as excinfo:
        parse_codelists(raw={"5": body}, fetched_at=_FETCHED_AT)
    assert excinfo.value.stage == "envelope"


@pytest.mark.unit
def test_codelist_not_a_list_raises_shape_error_at_envelope_stage() -> None:
    body = json.dumps({"codelists": {"codelist": {"not": "a list"}}}).encode()
    with pytest.raises(CodelistShapeError) as excinfo:
        parse_codelists(raw={"5": body}, fetched_at=_FETCHED_AT)
    assert excinfo.value.stage == "envelope"


@pytest.mark.unit
def test_empty_codelist_list_raises_shape_error_at_envelope_stage() -> None:
    body = json.dumps({"codelists": {"codelist": []}}).encode()
    with pytest.raises(CodelistShapeError) as excinfo:
        parse_codelists(raw={"5": body}, fetched_at=_FETCHED_AT)
    assert excinfo.value.stage == "envelope"


@pytest.mark.unit
def test_missing_codelist_items_raises_shape_error_at_envelope_stage() -> None:
    body = json.dumps({"codelists": {"codelist": [{"name": "Providers"}]}}).encode()
    with pytest.raises(CodelistShapeError) as excinfo:
        parse_codelists(raw={"5": body}, fetched_at=_FETCHED_AT)
    assert excinfo.value.stage == "envelope"


@pytest.mark.unit
def test_multi_block_payload_raises_shape_error_instead_of_silently_truncating() -> (
    None
):
    """A payload carrying more than one block is a real caller mistake, not a
    synthetic edge case: OECD's "All codes list" (id=0) response really does
    carry 26 blocks in one payload, so a caller who mistakenly replays that
    response for a single codelist_id must get a loud failure, not 213 rows
    quietly labelled with the wrong codelist_id."""
    body = json.dumps(
        {
            "codelists": {
                "codelist": [
                    {"name": "Providers", "codelist-items": {"codelist-item": []}},
                    {"name": "Recipients", "codelist-items": {"codelist-item": []}},
                ]
            }
        }
    ).encode()
    with pytest.raises(CodelistShapeError) as excinfo:
        parse_codelists(raw={"5": body}, fetched_at=_FETCHED_AT)
    assert excinfo.value.stage == "envelope"
    assert "2" in str(excinfo.value)
    assert excinfo.value.codelist_id == "5"


@pytest.mark.unit
def test_real_multi_block_all_codes_fixture_raises_shape_error() -> None:
    """`codelist_0.json` is a real captured OECD "All codes list" response (26
    blocks in one payload) -- confirms the multi-block guard fires on live
    data shaped this way, not just a synthetic two-block payload."""
    body = _load_fixture("codelist_0.json")
    with pytest.raises(CodelistShapeError) as excinfo:
        parse_codelists(raw={"5": body}, fetched_at=_FETCHED_AT)
    assert excinfo.value.stage == "envelope"
    assert "26" in str(excinfo.value)


# ---------------------------------------------------------------------------
# Zero rows: a self-consistency failure, not a shape one
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_zero_rows_for_a_requested_codelist_raises_validation_error() -> None:
    with pytest.raises(CodelistValidationError) as excinfo:
        parse_codelists(raw={"5": _envelope("Providers")}, fetched_at=_FETCHED_AT)
    assert excinfo.value.codelist_id == "5"


# ---------------------------------------------------------------------------
# Row failures
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_row_with_no_code_field_raises_shape_error_at_row_stage() -> None:
    row = {k: v for k, v in _ROW_1.items() if k != "code"}
    with pytest.raises(CodelistShapeError) as excinfo:
        parse_codelists(raw={"5": _envelope("Providers", row)}, fetched_at=_FETCHED_AT)
    assert excinfo.value.stage == "row"


@pytest.mark.unit
def test_row_with_no_derivable_label_raises_shape_error_at_row_stage() -> None:
    row = {**_ROW_1, "name": {"narrative": []}}
    with pytest.raises(CodelistShapeError) as excinfo:
        parse_codelists(raw={"5": _envelope("Providers", row)}, fetched_at=_FETCHED_AT)
    assert excinfo.value.stage == "row"


@pytest.mark.unit
def test_row_with_no_status_raises_shape_error_at_row_stage() -> None:
    row = {k: v for k, v in _ROW_1.items() if k != "status"}
    with pytest.raises(CodelistShapeError) as excinfo:
        parse_codelists(raw={"5": _envelope("Providers", row)}, fetched_at=_FETCHED_AT)
    assert excinfo.value.stage == "row"


@pytest.mark.unit
@pytest.mark.parametrize(
    "bad_date", ["01/01/2020", "2020-1-1", "not-a-date", "2020-01-01T00:00:00"]
)
def test_malformed_activation_date_raises_shape_error_at_row_stage(
    bad_date: str,
) -> None:
    """pyarrow's date parser is lenient, so the source format is asserted
    before casting rather than trusted."""
    row = {**_ROW_2, "activation-date": bad_date}
    with pytest.raises(CodelistShapeError) as excinfo:
        parse_codelists(
            raw={"13": _envelope("Recipients", row)}, fetched_at=_FETCHED_AT
        )
    assert excinfo.value.stage == "row"


@pytest.mark.unit
@pytest.mark.parametrize("bad_date", ["2026-99-99", "2026-02-30", "2021-13-01"])
def test_digit_shaped_but_impossible_activation_date_raises_shape_error(
    bad_date: str,
) -> None:
    """The regex only asserts digit-shape: "2026-99-99" matches
    `^\\d{4}-\\d{2}-\\d{2}$` but is not a real calendar date. Without the
    separate calendar check, this would reach `.astype("date32[pyarrow]")`
    unchecked, surfacing as an untyped pyarrow DateParseError instead of the
    documented CodelistShapeError."""
    row = {**_ROW_2, "activation-date": bad_date}
    with pytest.raises(CodelistShapeError) as excinfo:
        parse_codelists(
            raw={"13": _envelope("Recipients", row)}, fetched_at=_FETCHED_AT
        )
    assert excinfo.value.stage == "row"


@pytest.mark.unit
@pytest.mark.parametrize(
    "bad_value", [20200101, ["2020-01-01"], {"date": "2020-01-01"}]
)
def test_non_string_activation_date_raises_shape_error_not_type_error(
    bad_value: object,
) -> None:
    """The payload is JSON -- activation-date could be a number, list or
    dict. `.match()` on a non-string raises TypeError directly; that must
    not escape the typed CodelistShapeError boundary."""
    row = {**_ROW_2, "activation-date": bad_value}
    with pytest.raises(CodelistShapeError) as excinfo:
        parse_codelists(
            raw={"13": _envelope("Recipients", row)}, fetched_at=_FETCHED_AT
        )
    assert excinfo.value.stage == "row"


@pytest.mark.unit
def test_valid_activation_date_still_parses_as_control() -> None:
    row = {**_ROW_2, "activation-date": "1996-01-01"}
    snapshot = parse_codelists(
        raw={"13": _envelope("Recipients", row)}, fetched_at=_FETCHED_AT
    )
    assert snapshot.frame["activation_date"].astype(str).tolist() == ["1996-01-01"]


# ---------------------------------------------------------------------------
# fetched_at must be timezone-aware
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_naive_fetched_at_raises_validation_error() -> None:
    """`CodelistSnapshot.fetched_at` is documented as timezone-aware UTC, and
    `reconcile` later calls `.astimezone(UTC)` on it -- a naive value would be
    silently reinterpreted in the machine's local timezone."""
    with pytest.raises(CodelistValidationError):
        parse_codelists(
            raw={"5": _envelope("Providers", _ROW_1)},
            fetched_at=datetime(2026, 1, 1),  # no tzinfo
        )


# ---------------------------------------------------------------------------
# raw must not be empty
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_empty_raw_raises_validation_error() -> None:
    """An empty `raw` would otherwise sail through as a valid-looking
    zero-codelist snapshot -- `for codelist_id, body in raw.items()` simply
    never runs, so no envelope or row check ever fires. That is the same
    silent-data-loss shape a single codelist returning zero rows is already
    rejected for, just one level up: a caller replaying a missing or empty
    stored payload would get a snapshot `reconcile` could use to retire every
    row in a previous table instead of raising."""
    with pytest.raises(CodelistValidationError):
        parse_codelists(raw={}, fetched_at=_FETCHED_AT)


@pytest.mark.unit
def test_aware_utc_fetched_at_is_accepted() -> None:
    snapshot = parse_codelists(
        raw={"5": _envelope("Providers", _ROW_1)}, fetched_at=_FETCHED_AT
    )
    assert snapshot.fetched_at == _FETCHED_AT


@pytest.mark.unit
def test_aware_non_utc_fetched_at_is_accepted() -> None:
    non_utc = datetime(2026, 1, 1, tzinfo=timezone(timedelta(hours=-5)))
    snapshot = parse_codelists(
        raw={"5": _envelope("Providers", _ROW_1)}, fetched_at=non_utc
    )
    assert snapshot.fetched_at == non_utc


# ---------------------------------------------------------------------------
# Duplicate (codelist_id, code, activation_date) key handling
# ---------------------------------------------------------------------------
# _ROW_1 carries no `activation-date`, so both copies below key on (code="1",
# activation_date=None) — still a duplicate three-column key. `test_frame_contract.py`
# covers the validity-period case where the same code legitimately carries two *different*
# activation_dates.


@pytest.mark.unit
def test_identical_duplicate_rows_are_dropped_silently() -> None:
    raw = {"5": _envelope("Providers", _ROW_1, dict(_ROW_1))}
    snapshot = parse_codelists(raw=raw, fetched_at=_FETCHED_AT)
    assert len(snapshot.frame) == 1
    assert snapshot.frame["code"].tolist() == ["1"]


@pytest.mark.unit
def test_conflicting_duplicate_rows_raise_shape_error_at_row_stage() -> None:
    conflicting = {**_ROW_1, "dotstatcode": "DIFFERENT"}
    raw = {"5": _envelope("Providers", _ROW_1, conflicting)}
    with pytest.raises(CodelistShapeError) as excinfo:
        parse_codelists(raw=raw, fetched_at=_FETCHED_AT)
    assert excinfo.value.stage == "row"
    assert excinfo.value.codelist_id == "5"
    assert excinfo.value.body == raw["5"]


# ---------------------------------------------------------------------------
# unknown_statuses: passed through, never dropped
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_unknown_status_value_passes_through_and_is_recorded() -> None:
    row = {**_ROW_1, "status": "Suspended"}
    snapshot = parse_codelists(
        raw={"5": _envelope("Providers", row)}, fetched_at=_FETCHED_AT
    )
    assert snapshot.unknown_statuses == ("suspended",)
    assert snapshot.frame["status"].tolist() == ["suspended"]


@pytest.mark.unit
def test_unknown_statuses_are_deduplicated_in_first_seen_order() -> None:
    row_a = {**_ROW_1, "code": "1", "status": "Suspended"}
    row_b = {**_ROW_1, "code": "2", "iso-alpha-3-code": "BEL", "status": "Pending"}
    row_c = {**_ROW_1, "code": "3", "iso-alpha-3-code": "FRA", "status": "Suspended"}
    snapshot = parse_codelists(
        raw={"5": _envelope("Providers", row_a, row_b, row_c)}, fetched_at=_FETCHED_AT
    )
    assert snapshot.unknown_statuses == ("suspended", "pending")


# ---------------------------------------------------------------------------
# codelist_ids ordering and `raw` fidelity
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_codelist_ids_preserve_request_order() -> None:
    raw = {
        "13": _envelope("Recipients", _ROW_2),
        "5": _envelope("Providers", _ROW_1),
    }
    snapshot = parse_codelists(raw=raw, fetched_at=_FETCHED_AT)
    assert snapshot.codelist_ids == ("13", "5")


@pytest.mark.unit
def test_raw_carries_the_exact_bytes_and_cannot_be_edited_in_place() -> None:
    body = _envelope("Providers", _ROW_1)
    snapshot = parse_codelists(raw={"5": body}, fetched_at=_FETCHED_AT)
    assert snapshot.raw["5"] == body
    with pytest.raises(TypeError):
        snapshot.raw["5"] = b"tampered"  # type: ignore[index]


# ---------------------------------------------------------------------------
# parse_codelists opens no socket
# ---------------------------------------------------------------------------

_GUARD = """
import socket, sys

class _Blocked(socket.socket):
    def __init__(self, *a, **k):
        raise AssertionError("parse_codelists opened a socket")

socket.socket = _Blocked

def _audit(event, args):
    if event in ("socket.getaddrinfo", "socket.connect"):
        raise AssertionError(f"parse_codelists performed network activity: {event}")

sys.addaudithook(_audit)

import json
from datetime import datetime, timezone
from oda_reader.codelists import parse_codelists

body = json.dumps({
    "codelists": {
        "codelist": [{
            "codelist-items": {
                "codelist-item": [{
                    "status": "Active",
                    "code": "1",
                    "name": {"narrative": ["Austria"]},
                }]
            }
        }]
    }
}).encode()

snapshot = parse_codelists(raw={"5": body}, fetched_at=datetime(2026, 1, 1, tzinfo=timezone.utc))
assert snapshot.content_hash.startswith("v1:")
"""


@pytest.mark.unit
def test_parse_codelists_opens_no_socket() -> None:
    result = subprocess.run(
        [sys.executable, "-c", _GUARD], capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, result.stderr
