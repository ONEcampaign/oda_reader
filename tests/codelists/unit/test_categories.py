"""Offline tests for the category contract: `fetch_code_categories` / `parse_code_categories`.

Parallels `test_frame_contract.py` and `test_parse.py` for the area contract. The frame
contract under test here is the eleven-column shape keyed on
`(codelist_id, code, activation_date, crs, tossd)` rather than the area contract's
three-column key, because four codelists (Purpose code, Channel of delivery, Type of
finance, Co-operation modality) carry the same code with a different meaning per reporting
standard, distinguished only at the row level by `crs`/`tossd`.

`tests/fixtures/oecd/codelist_0.json` is the "All codes list" response: one JSON payload
holding all 26 codelist blocks, in the fixed order documented in survey.md (matching this
file's `_ALL_CODES_BLOCK_ORDER`). It is reconstructed here into 23 synthetic single-codelist
envelopes -- one per supported category id -- rather than requiring 23 separate fixture
files, and gives the breadth tests real OECD data across the whole supported set.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import pandas as pd
import pytest
from _helpers import envelope as _envelope
from _helpers import load_fixture as _load_fixture

from oda_reader.codelists import fetch_code_categories, parse_code_categories, reconcile
from oda_reader.codelists._parse import parse_codelists
from oda_reader.codelists._types import DEFAULT_URL, SUPPORTED_CATEGORY_IDS
from oda_reader.exceptions import CodelistShapeError, CodelistValidationError

_FETCHED_AT = datetime(2026, 1, 1, tzinfo=UTC)

# Columns redeclared here instead of imported, matching test_frame_contract.py's
# convention: importing would make the assertion tautological.
_CONTRACT_COLUMNS = (
    "codelist_id",
    "code",
    "label",
    "status",
    "crs",
    "tossd",
    "activation_date",
    "description",
    "category",
    "parent_code",
    "dac_reference",
)

# The 26 codelist_0.json blocks, in the order OECD returns them (verified against
# survey.md's inventory table, §2). Index 0 = Provider (5), 1 = Provider agency (16),
# 2 = Recipient (13) -- the three ids excluded from the category contract.
_ALL_CODES_BLOCK_ORDER: tuple[str, ...] = (
    "5",
    "16",
    "13",
    "6",
    "3",
    "1",
    "2",
    "15",
    "14",
    "10",
    "7",
    "23",
    "8",
    "9",
    "11",
    "12",
    "19",
    "17",
    "18",
    "21",
    "22",
    "4",
    "20",
    "24",
    "25",
    "26",
)


def _load_all_category_blocks() -> dict[str, bytes]:
    """Reconstruct one single-codelist envelope per supported category id from
    `codelist_0.json`'s 26-block "All codes list" response."""
    body = _load_fixture("codelist_0.json")
    payload = json.loads(body)
    blocks = payload["codelists"]["codelist"]
    assert len(blocks) == len(_ALL_CODES_BLOCK_ORDER)
    return {
        codelist_id: json.dumps({"codelists": {"codelist": [block]}}).encode()
        for codelist_id, block in zip(_ALL_CODES_BLOCK_ORDER, blocks, strict=True)
        if codelist_id in SUPPORTED_CATEGORY_IDS
    }


_ROW_A = {
    "status": "Active",
    "code": "1",
    "name": {
        "narrative": ["GNI: Gross National Income", {"xml:lang": "fr", "#text": "RNB"}]
    },
    "category": "0",
    "crs": "1",
    "tossd": "0",
}
_ROW_B = {
    "status": "active",
    "code": "2",
    "name": {"narrative": ["Loans", {"xml:lang": "fr", "#text": "Prêts"}]},
    "crs": "1",
    "tossd": "1",
    "activation-date": "1996-01-01",
}
_ROW_NO_OPTIONALS = {
    "status": "withdrawn",
    "code": "3",
    "name": {"narrative": ["Nowhere"]},
    "crs": "0",
    "tossd": "1",
    # no activation-date, description, category, parent-code, dac:reference
}

# Channel of delivery 11000: the same code carrying two different meanings under
# CRS and TOSSD (survey.md §1's worked example), differing only in crs/tossd/name.
_CHANNEL_CRS = {
    "status": "Active",
    "code": "11000",
    "name": {"narrative": ["Donor Government"]},
    "category": "10000",
    "crs": "1",
    "tossd": "0",
}
_CHANNEL_TOSSD = {
    **_CHANNEL_CRS,
    "name": {"narrative": ["Provider Government"]},
    "crs": "0",
    "tossd": "1",
}


# ---------------------------------------------------------------------------
# Column set, order, dtypes, grain
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_columns_are_exactly_the_eleven_in_documented_order() -> None:
    raw = {"21": _envelope("Concessionality", _ROW_A)}
    snapshot = parse_code_categories(raw=raw, fetched_at=_FETCHED_AT)
    assert tuple(snapshot.frame.columns) == _CONTRACT_COLUMNS


@pytest.mark.unit
def test_dtypes_are_string_pyarrow_except_date32_activation_date() -> None:
    raw = {"21": _envelope("Concessionality", _ROW_A, _ROW_B)}
    snapshot = parse_code_categories(raw=raw, fetched_at=_FETCHED_AT)
    for column in _CONTRACT_COLUMNS:
        if column == "activation_date":
            assert snapshot.frame[column].dtype == "date32[pyarrow]"
        else:
            assert snapshot.frame[column].dtype == "string[pyarrow]"


@pytest.mark.unit
def test_only_codelist_id_code_label_status_crs_tossd_are_never_null() -> None:
    raw = {"21": _envelope("Concessionality", _ROW_NO_OPTIONALS)}
    snapshot = parse_code_categories(raw=raw, fetched_at=_FETCHED_AT)
    row = snapshot.frame.iloc[0]
    for column in ("codelist_id", "code", "label", "status", "crs", "tossd"):
        assert row[column] is not pd.NA, f"{column} should never be null, got pd.NA"
    for column in (
        "activation_date",
        "description",
        "category",
        "parent_code",
        "dac_reference",
    ):
        assert row[column] is pd.NA, f"{column} should be pd.NA, got {row[column]!r}"


@pytest.mark.unit
def test_same_code_different_crs_tossd_are_kept_as_separate_rows() -> None:
    """Decision 1's worked example: Channel of delivery 11000 means something
    different under CRS and TOSSD, distinguished only by crs/tossd, not by
    activation_date -- the key must carry crs/tossd or these rows collide."""
    snapshot = parse_code_categories(
        raw={"3": _envelope("Channel of delivery", _CHANNEL_CRS, _CHANNEL_TOSSD)},
        fetched_at=_FETCHED_AT,
    )
    assert len(snapshot.frame) == 2
    assert snapshot.frame["code"].tolist() == ["11000", "11000"]
    assert sorted(snapshot.frame["label"].tolist()) == [
        "Donor Government",
        "Provider Government",
    ]
    assert sorted(zip(snapshot.frame["crs"], snapshot.frame["tossd"], strict=True)) == [
        ("0", "1"),
        ("1", "0"),
    ]


@pytest.mark.unit
def test_one_row_per_five_column_key_across_multiple_codelists() -> None:
    raw = {
        "21": _envelope("Concessionality", _ROW_A),
        "4": _envelope("Currency", {**_ROW_A, "code": "1"}),
    }
    snapshot = parse_code_categories(raw=raw, fetched_at=_FETCHED_AT)
    keys = list(
        zip(
            snapshot.frame["codelist_id"],
            snapshot.frame["code"],
            snapshot.frame["activation_date"],
            snapshot.frame["crs"],
            snapshot.frame["tossd"],
            strict=True,
        )
    )
    assert len(keys) == len(set(keys)) == 2


# ---------------------------------------------------------------------------
# dac:reference -> dac_reference, nested description unwrapping
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_dac_colon_reference_maps_to_dac_reference_column() -> None:
    row = {**_ROW_A, "dac:reference": "DCD/DAC/STAT(2018)23/REV3"}
    snapshot = parse_code_categories(
        raw={"21": _envelope("Concessionality", row)}, fetched_at=_FETCHED_AT
    )
    assert snapshot.frame["dac_reference"].tolist() == ["DCD/DAC/STAT(2018)23/REV3"]


@pytest.mark.unit
def test_description_is_unwrapped_like_name_selecting_english_narrative() -> None:
    row = {
        **_ROW_A,
        "description": {
            "narrative": [
                "English description",
                {"xml:lang": "fr", "#text": "Description française"},
            ]
        },
    }
    snapshot = parse_code_categories(
        raw={"21": _envelope("Concessionality", row)}, fetched_at=_FETCHED_AT
    )
    assert snapshot.frame["description"].tolist() == ["English description"]


# ---------------------------------------------------------------------------
# parent_code: empty string -> pd.NA (decision 2, load-bearing)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_parent_code_empty_string_becomes_pd_na_not_empty_string() -> None:
    row = {**_ROW_A, "parent-code": ""}
    snapshot = parse_code_categories(
        raw={"21": _envelope("Concessionality", row)}, fetched_at=_FETCHED_AT
    )
    assert snapshot.frame["parent_code"].iloc[0] is pd.NA


@pytest.mark.unit
def test_parent_code_real_value_is_kept() -> None:
    row = {**_ROW_A, "parent-code": "110"}
    snapshot = parse_code_categories(
        raw={"21": _envelope("Concessionality", row)}, fetched_at=_FETCHED_AT
    )
    assert snapshot.frame["parent_code"].iloc[0] == "110"


@pytest.mark.unit
def test_purpose_code_fixture_parent_code_notna_is_81_not_124() -> None:
    """The survey's load-bearing count: 124 of 392 rows carry the `parent-code` key,
    but 43 of those are the empty string. `.notna()` must see 81, not 124."""
    raw = {"10": _load_fixture("codelist_10.json")}
    snapshot = parse_code_categories(raw=raw, fetched_at=_FETCHED_AT)
    assert len(snapshot.frame) == 392
    assert snapshot.frame["parent_code"].notna().sum() == 81


# ---------------------------------------------------------------------------
# Real fixtures, unfiltered
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_channel_of_delivery_fixture_parses_to_905_rows_with_unique_key() -> None:
    raw = {"3": _load_fixture("codelist_3.json")}
    snapshot = parse_code_categories(raw=raw, fetched_at=_FETCHED_AT)
    assert len(snapshot.frame) == 905
    assert (
        snapshot.frame.duplicated(
            subset=["codelist_id", "code", "activation_date", "crs", "tossd"]
        ).sum()
        == 0
    )


@pytest.mark.unit
def test_type_of_finance_and_co_operation_modality_fixtures_parse_cleanly() -> None:
    raw = {
        "15": _load_fixture("codelist_15.json"),
        "14": _load_fixture("codelist_14.json"),
    }
    snapshot = parse_code_categories(raw=raw, fetched_at=_FETCHED_AT)
    assert len(snapshot.frame) == 104 + 77
    assert (
        snapshot.frame.duplicated(
            subset=["codelist_id", "code", "activation_date", "crs", "tossd"]
        ).sum()
        == 0
    )


@pytest.mark.unit
def test_concessionality_fixture_three_rows() -> None:
    raw = {"21": _load_fixture("codelist_21.json")}
    snapshot = parse_code_categories(raw=raw, fetched_at=_FETCHED_AT)
    assert len(snapshot.frame) == 3
    assert snapshot.frame["status"].tolist() == ["active", "active", "active"]


@pytest.mark.unit
def test_all_23_supported_category_ids_parse_cleanly_from_all_codes_fixture() -> None:
    """Breadth: every supported category id, reconstructed from the "All codes
    list" response, parses cleanly with a unique five-column key throughout."""
    raw = _load_all_category_blocks()
    assert set(raw) == set(SUPPORTED_CATEGORY_IDS)
    snapshot = parse_code_categories(raw=raw, fetched_at=_FETCHED_AT)
    assert (
        snapshot.frame.duplicated(
            subset=["codelist_id", "code", "activation_date", "crs", "tossd"]
        ).sum()
        == 0
    )
    assert set(snapshot.frame["codelist_id"]) == set(SUPPORTED_CATEGORY_IDS)


# ---------------------------------------------------------------------------
# content_hash: still "v1:"-prefixed, still stable/sensitive
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_content_hash_is_prefixed_v1() -> None:
    snapshot = parse_code_categories(
        raw={"21": _envelope("Concessionality", _ROW_A)}, fetched_at=_FETCHED_AT
    )
    assert snapshot.content_hash.startswith("v1:")


@pytest.mark.unit
def test_content_hash_stable_under_row_reordering() -> None:
    forward = parse_code_categories(
        raw={"21": _envelope("Concessionality", _ROW_A, _ROW_B)}, fetched_at=_FETCHED_AT
    )
    backward = parse_code_categories(
        raw={"21": _envelope("Concessionality", _ROW_B, _ROW_A)}, fetched_at=_FETCHED_AT
    )
    assert forward.content_hash == backward.content_hash


@pytest.mark.unit
def test_content_hash_matches_the_pinned_digest_for_codelist_21_fixture() -> None:
    """Pins the literal `content_hash` for `codelist_21.json` (Concessionality,
    3 rows) -- the category-contract counterpart of
    `test_frame_contract.py`'s area-contract pin. Every other content_hash test
    in this file only proves the hash reacts correctly to changes -- none of
    them would catch a canonicalisation bug that is wrong but self-consistent.
    If this literal ever needs to change, that means changing a published
    contract -- bump the `v1:` prefix to `v2:` per the design doc, don't just
    update the expected string here.
    """
    raw = {"21": _load_fixture("codelist_21.json")}
    snapshot = parse_code_categories(raw=raw, fetched_at=_FETCHED_AT)
    assert (
        snapshot.content_hash
        == "v1:0cac2670027404fb5b549e16a57a842865e1a224423dd6b5687498b44e7a7a3b"
    )


@pytest.mark.unit
def test_content_hash_changes_when_crs_tossd_differ_even_if_code_is_the_same() -> None:
    """Decision 1's key includes crs/tossd -- a hash computed over the wrong key
    would not distinguish the Channel of delivery 11000 case at all."""
    one_standard = parse_code_categories(
        raw={"3": _envelope("Channel of delivery", _CHANNEL_CRS)},
        fetched_at=_FETCHED_AT,
    )
    both_standards = parse_code_categories(
        raw={"3": _envelope("Channel of delivery", _CHANNEL_CRS, _CHANNEL_TOSSD)},
        fetched_at=_FETCHED_AT,
    )
    assert one_standard.content_hash != both_standards.content_hash


# ---------------------------------------------------------------------------
# fetched_at must be timezone-aware (same rule as parse_codelists)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_naive_fetched_at_raises_validation_error() -> None:
    with pytest.raises(CodelistValidationError):
        parse_code_categories(
            raw={"21": _envelope("Concessionality", _ROW_A)},
            fetched_at=datetime(2026, 1, 1),  # no tzinfo
        )


# ---------------------------------------------------------------------------
# Row/envelope failures reuse the same machinery as parse_codelists
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_row_with_no_code_field_raises_shape_error_at_row_stage() -> None:
    row = {k: v for k, v in _ROW_A.items() if k != "code"}
    with pytest.raises(CodelistShapeError) as excinfo:
        parse_code_categories(
            raw={"21": _envelope("Concessionality", row)}, fetched_at=_FETCHED_AT
        )
    assert excinfo.value.stage == "row"


@pytest.mark.unit
def test_row_with_no_crs_field_raises_shape_error_at_row_stage() -> None:
    row = {k: v for k, v in _ROW_A.items() if k != "crs"}
    with pytest.raises(CodelistShapeError) as excinfo:
        parse_code_categories(
            raw={"21": _envelope("Concessionality", row)}, fetched_at=_FETCHED_AT
        )
    assert excinfo.value.stage == "row"


@pytest.mark.unit
def test_row_with_no_tossd_field_raises_shape_error_at_row_stage() -> None:
    row = {k: v for k, v in _ROW_A.items() if k != "tossd"}
    with pytest.raises(CodelistShapeError) as excinfo:
        parse_code_categories(
            raw={"21": _envelope("Concessionality", row)}, fetched_at=_FETCHED_AT
        )
    assert excinfo.value.stage == "row"


@pytest.mark.unit
def test_zero_rows_for_a_requested_codelist_raises_validation_error() -> None:
    with pytest.raises(CodelistValidationError) as excinfo:
        parse_code_categories(
            raw={"21": _envelope("Concessionality")}, fetched_at=_FETCHED_AT
        )
    assert excinfo.value.codelist_id == "21"


@pytest.mark.unit
def test_conflicting_duplicate_key_raises_shape_error_at_row_stage() -> None:
    conflicting = {**_ROW_A, "category": "DIFFERENT"}
    raw = {"21": _envelope("Concessionality", _ROW_A, conflicting)}
    with pytest.raises(CodelistShapeError) as excinfo:
        parse_code_categories(raw=raw, fetched_at=_FETCHED_AT)
    assert excinfo.value.stage == "row"


@pytest.mark.unit
def test_identical_duplicate_key_is_dropped_silently() -> None:
    raw = {"21": _envelope("Concessionality", _ROW_A, dict(_ROW_A))}
    snapshot = parse_code_categories(raw=raw, fetched_at=_FETCHED_AT)
    assert len(snapshot.frame) == 1


@pytest.mark.unit
def test_not_json_raises_shape_error_at_envelope_stage() -> None:
    with pytest.raises(CodelistShapeError) as excinfo:
        parse_code_categories(raw={"21": b"not json at all"}, fetched_at=_FETCHED_AT)
    assert excinfo.value.stage == "envelope"


@pytest.mark.unit
def test_multi_block_payload_raises_shape_error_instead_of_silently_mislabelling() -> (
    None
):
    """A multi-block payload used to be silently truncated to its first block --
    worse here than in the area contract, since the truncated block need not even
    describe the category the caller asked for. `codelist_0.json` is a real captured
    OECD "All codes list" response (26 blocks in one payload): handing its bytes to
    `parse_code_categories` under, say, codelist_id "0" must fail loudly rather than
    emit 213 rows of *Provider* data labelled `codelist_id="0"`."""
    body = _load_fixture("codelist_0.json")
    with pytest.raises(CodelistShapeError) as excinfo:
        parse_code_categories(raw={"0": body}, fetched_at=_FETCHED_AT)
    assert excinfo.value.stage == "envelope"
    assert "26" in str(excinfo.value)


@pytest.mark.unit
def test_unknown_status_value_passes_through_and_is_recorded() -> None:
    row = {**_ROW_A, "status": "Suspended"}
    snapshot = parse_code_categories(
        raw={"21": _envelope("Concessionality", row)}, fetched_at=_FETCHED_AT
    )
    assert snapshot.unknown_statuses == ("suspended",)
    assert snapshot.frame["status"].tolist() == ["suspended"]


@pytest.mark.unit
def test_raw_carries_the_exact_bytes_and_cannot_be_edited_in_place() -> None:
    body = _envelope("Concessionality", _ROW_A)
    snapshot = parse_code_categories(raw={"21": body}, fetched_at=_FETCHED_AT)
    assert snapshot.raw["21"] == body
    with pytest.raises(TypeError):
        snapshot.raw["21"] = b"tampered"  # type: ignore[index]


# ---------------------------------------------------------------------------
# fetch_code_categories: validation before any request, no default codelist_ids
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_fetch_code_categories_has_no_default_codelist_ids() -> None:
    import inspect

    signature = inspect.signature(fetch_code_categories)
    assert signature.parameters["codelist_ids"].default is inspect.Parameter.empty


@pytest.mark.unit
def test_empty_codelist_ids_raises_validation_error() -> None:
    with pytest.raises(CodelistValidationError):
        fetch_code_categories(codelist_ids=[])


@pytest.mark.unit
def test_duplicate_codelist_ids_raises_validation_error() -> None:
    with pytest.raises(CodelistValidationError):
        fetch_code_categories(codelist_ids=["3", "3"])


@pytest.mark.unit
def test_unsupported_codelist_id_raises_validation_error() -> None:
    with pytest.raises(CodelistValidationError) as excinfo:
        fetch_code_categories(codelist_ids=["999"])
    assert excinfo.value.codelist_id == "999"


@pytest.mark.unit
@pytest.mark.parametrize("codelist_id", ["5", "13"])
def test_area_codelist_id_raises_validation_error_pointing_at_fetch_codelists(
    codelist_id: str,
) -> None:
    with pytest.raises(CodelistValidationError) as excinfo:
        fetch_code_categories(codelist_ids=[codelist_id])
    assert excinfo.value.codelist_id == codelist_id
    assert "fetch_codelists" in str(excinfo.value)


@pytest.mark.unit
def test_provider_agency_id_16_raises_validation_error_explaining_exclusion() -> None:
    with pytest.raises(CodelistValidationError) as excinfo:
        fetch_code_categories(codelist_ids=["16"])
    assert excinfo.value.codelist_id == "16"
    assert "donor" in str(excinfo.value).lower()


@pytest.mark.unit
def test_every_supported_category_id_is_accepted_by_validation() -> None:
    """No network call is made -- fetch_code_categories validates before touching
    the network, so a supported id must clear validation and reach the fetch
    loop. Proven here by monkeypatching the handshake to a no-network stub."""
    from oda_reader.codelists import _categories

    calls: list[str] = []

    def _fake_fetch(codelist_id: str, **_: Any) -> bytes:
        calls.append(codelist_id)
        return _envelope("Stub", {**_ROW_A, "code": codelist_id})

    original = _categories._fetch_codelist_bytes
    _categories._fetch_codelist_bytes = _fake_fetch  # type: ignore[assignment]
    try:
        snapshot = fetch_code_categories(codelist_ids=list(SUPPORTED_CATEGORY_IDS))
    finally:
        _categories._fetch_codelist_bytes = original  # type: ignore[assignment]

    assert calls == list(SUPPORTED_CATEGORY_IDS)
    assert snapshot.source_url == DEFAULT_URL
    assert set(snapshot.codelist_ids) == set(SUPPORTED_CATEGORY_IDS)


# ---------------------------------------------------------------------------
# reconcile must reject a category frame, not silently mis-key it
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_reconcile_rejects_a_category_snapshot_as_current() -> None:
    snapshot = parse_code_categories(
        raw={"21": _envelope("Concessionality", _ROW_A)}, fetched_at=_FETCHED_AT
    )
    with pytest.raises(CodelistValidationError) as excinfo:
        reconcile(previous=None, current=snapshot)
    assert "category" in str(excinfo.value).lower()


@pytest.mark.unit
def test_reconcile_still_accepts_an_area_snapshot_as_current() -> None:
    """Guards against a regression that would make _assert_area_contract_frame
    too strict and break the contract this whole package exists to serve."""
    area_row = {
        "status": "Active",
        "code": "1",
        "name": {"narrative": ["Austria"]},
        "type": "DAC member",
        "iso-alpha-3-code": "AUT",
        "dotstatcode": "AUT",
        "crs": "1",
        "tossd": "1",
    }
    snapshot = parse_codelists(
        raw={"5": _envelope("Providers", area_row)}, fetched_at=_FETCHED_AT
    )
    reconciliation = reconcile(previous=None, current=snapshot)
    assert len(reconciliation.table) == 1
