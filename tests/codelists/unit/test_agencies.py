"""Offline tests for the agency contract: `fetch_provider_agencies` / `parse_provider_agencies`.

Parallels `test_categories.py` for the category contract. The frame contract under test
here is the twelve-column shape keyed on `(codelist_id, donor_code, code,
activation_date)` -- donor-first, because an agency code is only meaningful within its
donor: code "1" alone means the Federal Ministry of Finance under one donor and Foreign
Affairs under another.

`tests/fixtures/oecd/codelist_0.json` is the "All codes list" response: one JSON payload
holding all 26 codelist blocks, in the fixed order `test_categories.py`'s
`_ALL_CODES_BLOCK_ORDER` documents, where index 1 is codelist 16 (Provider agency). It is
reconstructed here into a single-codelist envelope the same way `test_categories.py`'s
`_load_all_category_blocks` reconstructs all 23 category ones -- no dedicated
`codelist_16.json` fixture exists, because nobody has ever issued a standalone single-16
request. `test_live.py`'s network test closes that gap against the real OECD app.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pandas as pd
import pytest
from _helpers import envelope as _envelope
from _helpers import load_fixture as _load_fixture

from oda_reader.codelists import (
    fetch_provider_agencies,
    parse_provider_agencies,
    reconcile,
)
from oda_reader.codelists._categories import _validate_category_ids
from oda_reader.codelists._fetch import _validate_codelist_ids
from oda_reader.exceptions import CodelistShapeError, CodelistValidationError

_FETCHED_AT = datetime(2026, 1, 1, tzinfo=UTC)

# Columns redeclared here instead of imported, matching test_categories.py's
# convention -- importing would make the assertion tautological.
_AGENCY_CONTRACT_COLUMNS = (
    "codelist_id",
    "donor_code",
    "code",
    "label",
    "status",
    "acronym",
    "agency_type_code",
    "agency_type",
    "crs",
    "tossd",
    "used_in_cpa",
    "activation_date",
)

# codelist_0.json's block index 1 (verified against test_categories.py's
# _ALL_CODES_BLOCK_ORDER).
_AGENCY_BLOCK_INDEX = 1


def _load_agency_envelope() -> bytes:
    """Reconstruct a single-codelist envelope for codelist 16 (Provider agency)
    from block index 1 of `codelist_0.json`'s 26-block "All codes list" response --
    the pattern `test_categories.py`'s `_load_all_category_blocks` uses for every
    supported category id, applied here to the one block this contract owns."""
    body = _load_fixture("codelist_0.json")
    payload = json.loads(body)
    blocks = payload["codelists"]["codelist"]
    block = blocks[_AGENCY_BLOCK_INDEX]
    assert block["name"] == "Provider agency"
    return json.dumps({"codelists": {"codelist": [block]}}).encode()


_AGENCY_ROW_FULL = {
    "status": "Active",
    "code": "10",
    "donor-code": "1",
    "name": {
        "narrative": [
            "Ministry of Example",
            {"xml:lang": "fr", "#text": "Ministère d'exemple"},
        ]
    },
    "acronym": {"narrative": ["MOE", {"xml:lang": "fr", "#text": "MOE"}]},
    "Agencytype": {
        "code": "1",
        "name": {
            "narrative": [
                "Main Aid Agencies (in terms of budget)",
                {"_xml:lang": "fr", "#text": "Agences d'aide principales"},
            ]
        },
    },
    "crs": "1",
    "tossd": "1",
    "UsedinCPA": "1",
}
_AGENCY_ROW_NO_OPTIONALS = {
    "status": "withdrawn",
    "code": "20",
    "donor-code": "2",
    "name": {"narrative": ["Nowhere Agency"]},
    "Agencytype": {"code": "2", "name": {"narrative": ["Other"]}},
    "crs": "0",
    "tossd": "1",
    # no acronym, no UsedinCPA, no activation-date
}


# ---------------------------------------------------------------------------
# Column set, order, dtypes, nullability
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_columns_are_exactly_the_twelve_in_documented_order() -> None:
    raw = {"16": _envelope("Provider agency", _AGENCY_ROW_FULL)}
    snapshot = parse_provider_agencies(raw=raw, fetched_at=_FETCHED_AT)
    assert tuple(snapshot.frame.columns) == _AGENCY_CONTRACT_COLUMNS


@pytest.mark.unit
def test_dtypes_are_string_pyarrow_except_date32_activation_date() -> None:
    raw = {
        "16": _envelope("Provider agency", _AGENCY_ROW_FULL, _AGENCY_ROW_NO_OPTIONALS)
    }
    snapshot = parse_provider_agencies(raw=raw, fetched_at=_FETCHED_AT)
    for column in _AGENCY_CONTRACT_COLUMNS:
        if column == "activation_date":
            assert snapshot.frame[column].dtype == "date32[pyarrow]"
        else:
            assert snapshot.frame[column].dtype == "string[pyarrow]"


@pytest.mark.unit
def test_only_nine_columns_are_never_null_rest_are_nullable() -> None:
    raw = {"16": _envelope("Provider agency", _AGENCY_ROW_NO_OPTIONALS)}
    snapshot = parse_provider_agencies(raw=raw, fetched_at=_FETCHED_AT)
    row = snapshot.frame.iloc[0]
    never_null = (
        "codelist_id",
        "donor_code",
        "code",
        "label",
        "status",
        "agency_type_code",
        "agency_type",
        "crs",
        "tossd",
    )
    for column in never_null:
        assert row[column] is not pd.NA, f"{column} should never be null, got pd.NA"
    for column in ("acronym", "used_in_cpa", "activation_date"):
        assert row[column] is pd.NA, f"{column} should be pd.NA, got {row[column]!r}"


@pytest.mark.unit
def test_full_agency_fixture_parses_to_1374_rows_with_unique_key() -> None:
    snapshot = parse_provider_agencies(
        raw={"16": _load_agency_envelope()}, fetched_at=_FETCHED_AT
    )
    assert len(snapshot.frame) == 1374
    assert (
        snapshot.frame.duplicated(
            subset=["codelist_id", "donor_code", "code", "activation_date"]
        ).sum()
        == 0
    )
    # Every row's activation_date is null -- agency rows carry no activation-date
    # at all today (the module docstring's headline fact); the column stays in
    # the key regardless, ready for OECD adding validity periods later.
    assert snapshot.frame["activation_date"].isna().sum() == 1374
    assert snapshot.frame["acronym"].isna().sum() == 549
    assert snapshot.frame["used_in_cpa"].isna().sum() == 331


# ---------------------------------------------------------------------------
# Grain: an agency code is only meaningful within its donor
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_same_code_different_donors_yields_separate_rows_with_different_labels() -> (
    None
):
    """The worked example that justifies the whole contract (see `_agencies.py`'s
    module docstring): code "1" alone appears on 194 rows across 185 distinct
    labels, because an agency code means something different per donor. Asserted
    against real donor/label pairs, not just counts -- a bug that keyed on `code`
    alone would collapse this to one row and could still pass a count-only check."""
    snapshot = parse_provider_agencies(
        raw={"16": _load_agency_envelope()}, fetched_at=_FETCHED_AT
    )
    code_1 = snapshot.frame[snapshot.frame["code"] == "1"]
    assert len(code_1) == 194
    assert code_1["label"].nunique() == 185

    by_donor = dict(zip(code_1["donor_code"], code_1["label"], strict=True))
    assert by_donor["1"] == "Federal Ministry of Finance"
    assert by_donor["3"] == "Ministry of Foreign Affairs"
    assert by_donor["4"] == "Government"


@pytest.mark.unit
def test_empty_english_narrative_parses_to_empty_label_not_raising() -> None:
    """Deliberate, not a bug: donor 437's code "1" carries a real upstream row
    whose English narrative entry is the empty string (`{"narrative": ["",
    {...french...}]}`). `_narrative_text` returns `""` -- a `str`, so
    `_require_label` accepts it -- and the row survives with `label == ""`
    rather than raising. Dropping the row would discard a real agency the
    fixture proves exists, which is worse than carrying an empty label."""
    snapshot = parse_provider_agencies(
        raw={"16": _load_agency_envelope()}, fetched_at=_FETCHED_AT
    )
    row = snapshot.frame[
        (snapshot.frame["code"] == "1") & (snapshot.frame["donor_code"] == "437")
    ]
    assert len(row) == 1
    assert row["label"].iloc[0] == ""


# ---------------------------------------------------------------------------
# Nested Agencytype: projects both agency_type_code and agency_type
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_nested_agencytype_projects_code_and_english_label() -> None:
    snapshot = parse_provider_agencies(
        raw={"16": _envelope("Provider agency", _AGENCY_ROW_FULL)},
        fetched_at=_FETCHED_AT,
    )
    row = snapshot.frame.iloc[0]
    assert row["agency_type_code"] == "1"
    assert row["agency_type"] == "Main Aid Agencies (in terms of budget)"


@pytest.mark.unit
def test_agency_type_french_first_still_yields_english_label() -> None:
    """`Agencytype.name.narrative` tags its French entry with `_xml:lang`
    (underscore-prefixed), unlike the top-level `name`'s unprefixed `xml:lang`
    -- `_select_english_label` recognises both (see `_parse.py`). Without
    that, a French-first array here would silently return the French text:
    an unprefixed-only guard wouldn't recognise the French entry as tagged,
    so it would fall back to "first in the array" every time."""
    row = {
        **_AGENCY_ROW_FULL,
        "code": "99",
        "Agencytype": {
            "code": "9",
            "name": {
                "narrative": [
                    {"_xml:lang": "fr", "#text": "Agences francophones"},
                    "French-Language Agencies",
                ]
            },
        },
    }
    snapshot = parse_provider_agencies(
        raw={"16": _envelope("Provider agency", row)}, fetched_at=_FETCHED_AT
    )
    assert snapshot.frame["agency_type"].iloc[0] == "French-Language Agencies"


@pytest.mark.unit
@pytest.mark.parametrize(
    "mutate",
    [
        lambda row: {k: v for k, v in row.items() if k != "Agencytype"},
        lambda row: {**row, "Agencytype": "not-a-dict"},
        lambda row: {**row, "Agencytype": {"name": row["Agencytype"]["name"]}},
        lambda row: {**row, "Agencytype": {**row["Agencytype"], "code": ""}},
        lambda row: {**row, "Agencytype": {"code": "1"}},
    ],
    ids=["missing", "not-a-dict", "no-code", "empty-code", "no-name"],
)
def test_missing_or_malformed_agency_type_raises_shape_error_at_row_stage(
    mutate,
) -> None:
    row = mutate(_AGENCY_ROW_FULL)
    with pytest.raises(CodelistShapeError) as excinfo:
        parse_provider_agencies(
            raw={"16": _envelope("Provider agency", row)}, fetched_at=_FETCHED_AT
        )
    assert excinfo.value.stage == "row"


# ---------------------------------------------------------------------------
# status: lowercased, unknown values passed through and recorded
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_full_fixture_statuses_are_all_active_and_unknown_statuses_is_empty() -> None:
    snapshot = parse_provider_agencies(
        raw={"16": _load_agency_envelope()}, fetched_at=_FETCHED_AT
    )
    assert set(snapshot.frame["status"]) == {"active"}
    assert snapshot.unknown_statuses == ()


@pytest.mark.unit
def test_unknown_status_value_passes_through_and_is_recorded() -> None:
    row = {**_AGENCY_ROW_FULL, "status": "Suspended"}
    snapshot = parse_provider_agencies(
        raw={"16": _envelope("Provider agency", row)}, fetched_at=_FETCHED_AT
    )
    assert snapshot.unknown_statuses == ("suspended",)
    assert snapshot.frame["status"].tolist() == ["suspended"]


# ---------------------------------------------------------------------------
# Duplicate (donor_code, code, activation_date) key handling
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_identical_duplicate_key_is_dropped_silently() -> None:
    raw = {"16": _envelope("Provider agency", _AGENCY_ROW_FULL, dict(_AGENCY_ROW_FULL))}
    snapshot = parse_provider_agencies(raw=raw, fetched_at=_FETCHED_AT)
    assert len(snapshot.frame) == 1


@pytest.mark.unit
def test_conflicting_duplicate_key_raises_shape_error_at_row_stage() -> None:
    conflicting = {**_AGENCY_ROW_FULL, "status": "Withdrawn"}
    raw = {"16": _envelope("Provider agency", _AGENCY_ROW_FULL, conflicting)}
    with pytest.raises(CodelistShapeError) as excinfo:
        parse_provider_agencies(raw=raw, fetched_at=_FETCHED_AT)
    assert excinfo.value.stage == "row"


@pytest.mark.unit
def test_zero_rows_for_the_requested_codelist_raises_validation_error() -> None:
    with pytest.raises(CodelistValidationError) as excinfo:
        parse_provider_agencies(
            raw={"16": _envelope("Provider agency")}, fetched_at=_FETCHED_AT
        )
    assert excinfo.value.codelist_id == "16"


# ---------------------------------------------------------------------------
# fetched_at must be timezone-aware (same rule as the other two contracts)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_naive_fetched_at_raises_validation_error() -> None:
    with pytest.raises(CodelistValidationError):
        parse_provider_agencies(
            raw={"16": _envelope("Provider agency", _AGENCY_ROW_FULL)},
            fetched_at=datetime(2026, 1, 1),  # no tzinfo
        )


# ---------------------------------------------------------------------------
# raw must not be empty (same rule as the other two contracts)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_empty_raw_raises_validation_error() -> None:
    """An empty `raw` must not sail through as a valid-looking
    zero-codelist snapshot -- see the parse_codelists test of the same name
    for the full rationale."""
    with pytest.raises(CodelistValidationError):
        parse_provider_agencies(raw={}, fetched_at=_FETCHED_AT)


# ---------------------------------------------------------------------------
# content_hash: "v1:"-prefixed, order-independent, and pinned
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_content_hash_is_prefixed_v1() -> None:
    snapshot = parse_provider_agencies(
        raw={"16": _envelope("Provider agency", _AGENCY_ROW_FULL)},
        fetched_at=_FETCHED_AT,
    )
    assert snapshot.content_hash.startswith("v1:")


@pytest.mark.unit
def test_content_hash_stable_under_row_reordering() -> None:
    forward = parse_provider_agencies(
        raw={
            "16": _envelope(
                "Provider agency", _AGENCY_ROW_FULL, _AGENCY_ROW_NO_OPTIONALS
            )
        },
        fetched_at=_FETCHED_AT,
    )
    backward = parse_provider_agencies(
        raw={
            "16": _envelope(
                "Provider agency", _AGENCY_ROW_NO_OPTIONALS, _AGENCY_ROW_FULL
            )
        },
        fetched_at=_FETCHED_AT,
    )
    assert forward.content_hash == backward.content_hash


@pytest.mark.unit
def test_content_hash_matches_the_pinned_digest_for_the_full_agency_fixture() -> None:
    """Pins the literal `content_hash` for the reconstructed 1,374-row agency
    payload -- the agency-contract counterpart of
    `test_categories.py:372`'s pin for codelist 21. Every other content_hash
    test in this file only proves the hash reacts correctly to changes --
    none of them would catch a canonicalisation bug that is wrong but
    self-consistent. If this literal ever needs to change, that means
    changing a published contract -- bump the `v1:` prefix to `v2:`, don't
    just update the expected string here.
    """
    snapshot = parse_provider_agencies(
        raw={"16": _load_agency_envelope()}, fetched_at=_FETCHED_AT
    )
    assert (
        snapshot.content_hash
        == "v1:96eaea0fcd0b9c5cda52ede3ffc1637161e50ec3454902c99b0922e4ec613462"
    )


# ---------------------------------------------------------------------------
# fetch_provider_agencies: no codelist_ids parameter
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_fetch_provider_agencies_has_no_codelist_ids_parameter() -> None:
    import inspect

    signature = inspect.signature(fetch_provider_agencies)
    assert "codelist_ids" not in signature.parameters


@pytest.mark.unit
def test_passing_codelist_ids_to_fetch_provider_agencies_raises_type_error() -> None:
    with pytest.raises(TypeError):
        fetch_provider_agencies(codelist_ids=["16"])  # type: ignore[call-arg]


# ---------------------------------------------------------------------------
# reconcile must reject an agency frame, not silently mis-key it
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_reconcile_rejects_an_agency_snapshot_as_current() -> None:
    snapshot = parse_provider_agencies(
        raw={"16": _envelope("Provider agency", _AGENCY_ROW_FULL)},
        fetched_at=_FETCHED_AT,
    )
    with pytest.raises(CodelistValidationError) as excinfo:
        reconcile(previous=None, current=snapshot)
    detail = str(excinfo.value)
    assert "contract" in detail
    assert "'agency'" in detail


# ---------------------------------------------------------------------------
# "16" redirects to the agency contract from both other contracts' validators
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.parametrize(
    "validator",
    [_validate_category_ids, _validate_codelist_ids],
    ids=["category", "area"],
)
def test_validator_redirects_agency_id_to_fetch_provider_agencies(validator) -> None:
    with pytest.raises(CodelistValidationError) as excinfo:
        validator(("16",))
    assert excinfo.value.codelist_id == "16"
    assert "fetch_provider_agencies" in str(excinfo.value)
