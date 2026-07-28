"""The frame contract and `content_hash`.

This test file freezes the public return contract under semver: the ten-column set and order,
the two dtypes, `(codelist_id, code, activation_date)` uniqueness, `pd.NA` as the null
representation, and `content_hash`'s stability/sensitivity properties. These assertions carry
more weight than ordinary test hygiene and must not be casually refactored.

**The grain is `(codelist_id, code, activation_date)`, not `(codelist_id, code)`.** OECD
publishes codelist 13 as validity periods: 116 of 207 codes carry two to six rows apiece,
each a distinct period of that code's history (code 130, Algeria, has three: withdrawn
1996-2010, withdrawn 2011-2021, active from 2022). Those rows represent one entity's history,
not duplicates, and all of them belong in the frame. Measured on the committed fixtures:
577 rows across both codelists, zero duplicate three-column keys — every test below that
touches the real fixtures runs against them unfiltered.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd
import pytest
from _helpers import envelope as _envelope
from _helpers import load_fixture as _load_fixture

from oda_reader.codelists import parse_codelists
from oda_reader.exceptions import CodelistShapeError

_FETCHED_AT = datetime(2026, 1, 1, tzinfo=UTC)

# Columns are redeclared here instead of imported from the implementation.
# Importing would make the assertion tautological — it would prove only that the test
# and implementation agree, not that they match the frozen contract.
_CONTRACT_COLUMNS = (
    "codelist_id",
    "code",
    "label",
    "status",
    "type",
    "dotstat_code",
    "iso3",
    "crs",
    "tossd",
    "activation_date",
)


_ROW_A = {
    "status": "Active",
    "code": "1",
    "name": {"narrative": ["Austria", {"xml:lang": "fr", "#text": "Autriche"}]},
    "type": "DAC member",
    "iso-alpha-3-code": "AUT",
    "dotstatcode": "AUT",
    "crs": "1",
    "tossd": "1",
}
_ROW_B = {
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
_ROW_NO_OPTIONALS = {
    "status": "withdrawn",
    "code": "3",
    "name": {"narrative": ["Nowhere"]},
    # no type, dotstatcode, iso-alpha-3-code, crs, tossd, activation-date
}

# Algeria-shaped: two validity periods of the same code, differing only in
# `status` and `activation_date`.
_PERIOD_1 = {
    "status": "withdrawn",
    "code": "130",
    "name": {"narrative": ["Algeria", {"xml:lang": "fr", "#text": "Algérie"}]},
    "type": "Country",
    "iso-alpha-3-code": "DZA",
    "dotstatcode": "DZA",
    "crs": "1",
    "tossd": "1",
    "activation-date": "1996-01-01",
}
_PERIOD_2 = {**_PERIOD_1, "status": "active", "activation-date": "2022-01-01"}


# ---------------------------------------------------------------------------
# Column set, order, dtypes, grain
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_columns_are_exactly_the_ten_in_documented_order() -> None:
    raw = {"5": _envelope("Providers", _ROW_A)}
    snapshot = parse_codelists(raw=raw, fetched_at=_FETCHED_AT)
    assert tuple(snapshot.frame.columns) == _CONTRACT_COLUMNS


@pytest.mark.unit
def test_dtypes_are_string_pyarrow_except_date32_activation_date() -> None:
    raw = {"5": _envelope("Providers", _ROW_A, _ROW_B)}
    snapshot = parse_codelists(raw=raw, fetched_at=_FETCHED_AT)
    for column in _CONTRACT_COLUMNS:
        if column == "activation_date":
            assert snapshot.frame[column].dtype == "date32[pyarrow]"
        else:
            assert snapshot.frame[column].dtype == "string[pyarrow]"


@pytest.mark.unit
def test_one_row_per_codelist_id_code_activation_date_across_multiple_codelists() -> (
    None
):
    raw = {
        "5": _envelope("Providers", _ROW_A),
        "13": _envelope("Recipients", {**_ROW_A, "status": "withdrawn"}),
    }
    snapshot = parse_codelists(raw=raw, fetched_at=_FETCHED_AT)
    # Same `code` ("1") in both codelists is legitimate: codelist_id is part of the key.
    keys = list(
        zip(
            snapshot.frame["codelist_id"],
            snapshot.frame["code"],
            snapshot.frame["activation_date"],
            strict=True,
        )
    )
    assert len(keys) == len(set(keys)) == 2


@pytest.mark.unit
def test_same_code_different_activation_date_are_both_kept_as_separate_rows() -> None:
    """The validity-period case: two periods of one code, not a duplicate."""
    snapshot = parse_codelists(
        raw={"13": _envelope("Recipients", _PERIOD_1, _PERIOD_2)},
        fetched_at=_FETCHED_AT,
    )
    assert len(snapshot.frame) == 2
    assert snapshot.frame["code"].tolist() == ["130", "130"]
    assert sorted(snapshot.frame["status"].tolist()) == ["active", "withdrawn"]
    assert snapshot.frame["activation_date"].astype(str).tolist() == [
        "1996-01-01",
        "2022-01-01",
    ]


@pytest.mark.unit
def test_missing_optional_fields_are_pd_na_not_empty_string() -> None:
    raw = {"5": _envelope("Providers", _ROW_NO_OPTIONALS)}
    snapshot = parse_codelists(raw=raw, fetched_at=_FETCHED_AT)
    row = snapshot.frame.iloc[0]
    for column in ("type", "dotstat_code", "iso3", "crs", "tossd", "activation_date"):
        assert row[column] is pd.NA, f"{column} should be pd.NA, got {row[column]!r}"


# ---------------------------------------------------------------------------
# Real fixtures, unfiltered — the whole point of having captured them live
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_full_codelist_13_fixture_parses_cleanly_to_364_rows() -> None:
    """The real, all-status capture: 207 distinct codes, 116 with 2-6 validity periods each."""
    raw = {"13": _load_fixture("codelist_13.json")}
    snapshot = parse_codelists(raw=raw, fetched_at=_FETCHED_AT)
    assert len(snapshot.frame) == 364
    assert snapshot.frame.duplicated(subset=["code", "activation_date"]).sum() == 0


@pytest.mark.unit
def test_both_fixtures_together_yield_577_rows_with_no_duplicate_keys() -> None:
    raw = {
        "5": _load_fixture("codelist_5.json"),
        "13": _load_fixture("codelist_13.json"),
    }
    snapshot = parse_codelists(raw=raw, fetched_at=_FETCHED_AT)
    assert len(snapshot.frame) == 577
    assert (
        snapshot.frame.duplicated(
            subset=["codelist_id", "code", "activation_date"]
        ).sum()
        == 0
    )


@pytest.mark.unit
def test_codelist_5_fixture_activation_date_null_for_every_row() -> None:
    raw = {"5": _load_fixture("codelist_5.json")}
    snapshot = parse_codelists(raw=raw, fetched_at=_FETCHED_AT)
    assert snapshot.frame["activation_date"].isna().all()


@pytest.mark.unit
def test_codelist_5_fixture_crs_and_tossd_non_null_for_every_row() -> None:
    raw = {"5": _load_fixture("codelist_5.json")}
    snapshot = parse_codelists(raw=raw, fetched_at=_FETCHED_AT)
    assert snapshot.frame["crs"].notna().all()
    assert snapshot.frame["tossd"].notna().all()


@pytest.mark.unit
def test_codelist_5_and_13_active_statuses_both_normalise_to_lowercase() -> None:
    """Codelist 5 says "Active", codelist 13 says "active" — both are normalized to lowercase."""
    raw = {
        "5": _load_fixture("codelist_5.json"),
        "13": _load_fixture("codelist_13.json"),
    }
    snapshot = parse_codelists(raw=raw, fetched_at=_FETCHED_AT)
    active_rows = snapshot.frame[snapshot.frame["status"] == "active"]
    # 213 codelist-5 rows (all "Active") + 177 codelist-13 rows (all "active").
    assert len(active_rows) == 390
    assert snapshot.unknown_statuses == ()


# ---------------------------------------------------------------------------
# Duplicate-key handling on the three-column grain
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_identical_key_with_byte_identical_values_dropped_silently() -> None:
    raw = {"5": _envelope("Providers", _ROW_A, dict(_ROW_A))}
    snapshot = parse_codelists(raw=raw, fetched_at=_FETCHED_AT)
    assert len(snapshot.frame) == 1


@pytest.mark.unit
def test_identical_key_with_conflicting_values_raises_shape_error_at_row_stage() -> (
    None
):
    conflicting = {**_ROW_A, "dotstatcode": "DIFFERENT"}
    raw = {"5": _envelope("Providers", _ROW_A, conflicting)}
    with pytest.raises(CodelistShapeError) as excinfo:
        parse_codelists(raw=raw, fetched_at=_FETCHED_AT)
    assert excinfo.value.stage == "row"


# ---------------------------------------------------------------------------
# content_hash
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_content_hash_is_prefixed_v1() -> None:
    raw = {"5": _envelope("Providers", _ROW_A)}
    snapshot = parse_codelists(raw=raw, fetched_at=_FETCHED_AT)
    assert snapshot.content_hash.startswith("v1:")


@pytest.mark.unit
def test_content_hash_stable_across_two_builds_of_the_same_frame() -> None:
    raw = {"5": _envelope("Providers", _ROW_A, _ROW_B)}
    first = parse_codelists(raw=raw, fetched_at=_FETCHED_AT)
    second = parse_codelists(raw=raw, fetched_at=_FETCHED_AT)
    assert first.content_hash == second.content_hash


@pytest.mark.unit
def test_content_hash_stable_under_row_reordering() -> None:
    forward = parse_codelists(
        raw={"5": _envelope("Providers", _ROW_A, _ROW_B)}, fetched_at=_FETCHED_AT
    )
    backward = parse_codelists(
        raw={"5": _envelope("Providers", _ROW_B, _ROW_A)}, fetched_at=_FETCHED_AT
    )
    assert forward.content_hash == backward.content_hash


@pytest.mark.unit
def test_content_hash_stable_under_reordering_with_na_and_real_activation_dates() -> (
    None
):
    """Sorting on the third key column must handle a pd.NA `activation_date` correctly
    -- it sorts first, rendering as the \\x00 sentinel -- exercised here with two periods
    of the same code, one of them null."""
    period_no_date = {**_PERIOD_1, "activation-date": None}
    del period_no_date["activation-date"]
    forward = parse_codelists(
        raw={"13": _envelope("Recipients", period_no_date, _PERIOD_2)},
        fetched_at=_FETCHED_AT,
    )
    backward = parse_codelists(
        raw={"13": _envelope("Recipients", _PERIOD_2, period_no_date)},
        fetched_at=_FETCHED_AT,
    )
    assert forward.content_hash == backward.content_hash


@pytest.mark.unit
def test_content_hash_changes_when_a_single_value_changes() -> None:
    baseline = parse_codelists(
        raw={"5": _envelope("Providers", _ROW_A)}, fetched_at=_FETCHED_AT
    )
    changed_row = {**_ROW_A, "dotstatcode": "DIFFERENT"}
    changed = parse_codelists(
        raw={"5": _envelope("Providers", changed_row)}, fetched_at=_FETCHED_AT
    )
    assert baseline.content_hash != changed.content_hash


@pytest.mark.unit
def test_content_hash_matches_the_pinned_digest_for_the_committed_fixtures() -> None:
    """Pins the literal `content_hash` for `codelist_5.json` + `codelist_13.json`
    together (the same 577-row pair `test_both_fixtures_together_yield_577_rows_...`
    exercises above), not just its stability/sensitivity properties.

    Every other content_hash test in this file only proves the hash reacts
    correctly to changes -- none of them would catch a canonicalisation bug that
    is wrong but self-consistent (e.g. a different field order, separator, or
    encoding that is still stable and still sensitive to value changes). This is
    the one test that would fail if the hash recipe silently changed. If this
    literal ever needs to change, that means changing a published contract -- bump
    the `v1:` prefix to `v2:` rather than editing the expectation.
    """
    raw = {
        "5": _load_fixture("codelist_5.json"),
        "13": _load_fixture("codelist_13.json"),
    }
    snapshot = parse_codelists(raw=raw, fetched_at=_FETCHED_AT)
    assert (
        snapshot.content_hash
        == "v1:09a52c16e5bd4238d86f5c0e9617829ecfff70313401168cd41fac1dc682d69c"
    )


@pytest.mark.unit
def test_content_hash_na_sentinel_is_not_pd_nas_own_repr() -> None:
    """The \\x00 sentinel must come from an explicit `pd.isna` branch, not `str(pd.NA)`.

    A plausible implementation bug is rendering a missing value by falling through to
    `str(value)` without checking `pd.isna` first, which would stringify `pd.NA` as the four
    characters `<NA>` rather than the documented `\\x00` sentinel — and a source value that
    happened to equal `"<NA>"` would then collide with a genuinely missing one. This proves the
    sentinel path is taken: a `pd.NA` cell and a cell holding the string pandas prints for `pd.NA`
    hash differently.
    """
    missing_dotstat = parse_codelists(
        raw={"5": _envelope("Providers", _ROW_NO_OPTIONALS)}, fetched_at=_FETCHED_AT
    )
    looks_like_na_repr = {**_ROW_NO_OPTIONALS, "dotstatcode": str(pd.NA)}
    literal_string = parse_codelists(
        raw={"5": _envelope("Providers", looks_like_na_repr)}, fetched_at=_FETCHED_AT
    )
    assert missing_dotstat.content_hash != literal_string.content_hash
