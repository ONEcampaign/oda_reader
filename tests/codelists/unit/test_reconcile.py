"""Offline tests for `reconcile` and `Reconciliation` — the history surface.

Every case here guards one of the reconciliation rules; a regression in any of them is a
data-corruption bug for a warehouse consumer, not a cosmetic one. Fixtures are built inline
(`_snapshot`) rather than stored as files — a stored frame would be a file to keep in sync
for no readability gain.

**The key is `(codelist_id, code, activation_date)`, and `activation_date` is nullable.**
`pd.DataFrame.merge` and `pd.DataFrame.duplicated` both match/detect `pd.NA` keys correctly
on this repo's pinned stack (pandas 2.3.3, pyarrow 22.0.0), so the tests below exercise that
directly against the real 577-row fixture frame rather than only against small synthetic
frames.
"""

from __future__ import annotations

import pathlib
import random
from datetime import UTC, date, datetime
from types import MappingProxyType

import pandas as pd
import pytest

from oda_reader.codelists import parse_codelists, reconcile
from oda_reader.codelists._types import CodelistSnapshot
from oda_reader.exceptions import CodelistValidationError

_FIXTURES = pathlib.Path(__file__).parent.parent.parent / "fixtures" / "oecd"

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


def _load_fixture_raw() -> dict[str, bytes]:
    paths = {c: _FIXTURES / f"codelist_{c}.json" for c in ("5", "13")}
    for path in paths.values():
        if not path.exists():
            pytest.skip(
                f"Fixture {path.name} not yet captured — run --capture-fixtures first"
            )
    return {c: p.read_bytes() for c, p in paths.items()}


def _snapshot(
    rows: list[dict],
    *,
    fetched_at: datetime,
    codelist_ids: tuple[str, ...] = ("5",),
    source_url: str = "https://example.test/CodesList.aspx",
) -> CodelistSnapshot:
    """Build a `CodelistSnapshot` directly from row dicts, bypassing `parse_codelists`.

    Each row supplies `code` and any contract column it wants to override; everything else
    defaults to a minimal valid value. Since `reconcile` only consumes `.contract`, `.frame`,
    `.fetched_at` and `.codelist_ids`, there's no need to round-trip through the real OECD
    envelope shape.
    """
    defaults = {
        "codelist_id": "5",
        "label": None,
        "status": "active",
        "type": pd.NA,
        "dotstat_code": pd.NA,
        "iso3": pd.NA,
        "crs": "1",
        "tossd": "1",
        "activation_date": pd.NA,
    }
    records = []
    for row in rows:
        merged = {**defaults, **row}
        if merged["label"] is None:
            merged["label"] = f"Label {merged['code']}"
        records.append(merged)
    frame = pd.DataFrame.from_records(records, columns=list(_CONTRACT_COLUMNS))
    for column in _CONTRACT_COLUMNS:
        if column == "activation_date":
            frame[column] = frame[column].astype("date32[pyarrow]")
        else:
            frame[column] = frame[column].astype("string[pyarrow]")
    return CodelistSnapshot(
        contract="area",
        frame=frame,
        raw=MappingProxyType({}),
        fetched_at=fetched_at,
        source_url=source_url,
        codelist_ids=codelist_ids,
        content_hash="v1:test",
        unknown_statuses=(),
    )


# ---------------------------------------------------------------------------
# Base cases
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_first_run_puts_every_row_in_added_and_none_in_retired_or_changed() -> None:
    snap = _snapshot(
        [{"code": "1"}, {"code": "2"}], fetched_at=datetime(2026, 1, 1, tzinfo=UTC)
    )
    rec = reconcile(previous=None, current=snap)

    assert len(rec.table) == 2
    assert len(rec.added) == 2
    assert rec.retired.empty
    assert rec.changed.empty
    assert rec.is_unchanged is False
    assert (rec.table["presence"] == "current").all()
    assert (rec.table["first_seen"] == date(2026, 1, 1)).all()
    assert (rec.table["last_seen"] == date(2026, 1, 1)).all()
    assert (rec.table["source_status"] == rec.table["status"]).all()


@pytest.mark.unit
def test_unchanged_rerun_reports_is_unchanged_true_and_advances_last_seen() -> None:
    snap1 = _snapshot([{"code": "1"}], fetched_at=datetime(2026, 1, 1, tzinfo=UTC))
    rec1 = reconcile(previous=None, current=snap1)

    snap2 = _snapshot([{"code": "1"}], fetched_at=datetime(2026, 2, 1, tzinfo=UTC))
    rec2 = reconcile(previous=rec1.table, current=snap2)

    assert rec2.is_unchanged is True
    assert rec2.added.empty
    assert rec2.retired.empty
    assert rec2.changed.empty
    assert (rec2.table["last_seen"] == date(2026, 2, 1)).all()


@pytest.mark.unit
def test_a_new_code_lands_in_added_only() -> None:
    snap1 = _snapshot([{"code": "1"}], fetched_at=datetime(2026, 1, 1, tzinfo=UTC))
    rec1 = reconcile(previous=None, current=snap1)

    snap2 = _snapshot(
        [{"code": "1"}, {"code": "2"}], fetched_at=datetime(2026, 2, 1, tzinfo=UTC)
    )
    rec2 = reconcile(previous=rec1.table, current=snap2)

    assert list(rec2.added["code"]) == ["2"]
    assert rec2.retired.empty
    assert rec2.changed.empty
    assert rec2.is_unchanged is False


@pytest.mark.unit
def test_a_changed_value_lands_in_changed_with_old_and_new() -> None:
    snap1 = _snapshot(
        [{"code": "1", "label": "Old Label"}],
        fetched_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    rec1 = reconcile(previous=None, current=snap1)

    snap2 = _snapshot(
        [{"code": "1", "label": "New Label"}],
        fetched_at=datetime(2026, 2, 1, tzinfo=UTC),
    )
    rec2 = reconcile(previous=rec1.table, current=snap2)

    assert len(rec2.changed) == 1
    row = rec2.changed.iloc[0]
    assert row["column"] == "label"
    assert row["old_value"] == "Old Label"
    assert row["new_value"] == "New Label"
    assert row["observed_at"] == date(2026, 2, 1)
    assert rec2.added.empty
    assert rec2.retired.empty


@pytest.mark.unit
def test_a_disappearing_code_is_retired_not_dropped() -> None:
    snap1 = _snapshot(
        [{"code": "1"}, {"code": "2"}], fetched_at=datetime(2026, 1, 1, tzinfo=UTC)
    )
    rec1 = reconcile(previous=None, current=snap1)

    snap2 = _snapshot([{"code": "1"}], fetched_at=datetime(2026, 2, 1, tzinfo=UTC))
    rec2 = reconcile(previous=rec1.table, current=snap2)

    assert len(rec2.table) == 2  # rule 1: nothing is ever dropped
    assert list(rec2.retired["code"]) == ["2"]
    retired_row = rec2.table[rec2.table["code"] == "2"].iloc[0]
    assert retired_row["presence"] == "retired"
    assert retired_row["last_seen"] == date(2026, 1, 1)  # frozen, not advanced
    assert retired_row["first_seen"] == date(2026, 1, 1)


@pytest.mark.unit
def test_previous_with_no_lineage_columns_gets_first_seen_na_not_today() -> None:
    snap = _snapshot([{"code": "1"}], fetched_at=datetime(2026, 1, 1, tzinfo=UTC))
    bare_previous = snap.frame.copy()  # the ten contract columns, no lineage

    rec = reconcile(previous=bare_previous, current=snap)

    assert rec.table["first_seen"].isna().all()  # rule 10: never today
    assert (rec.table["presence"] == "current").all()
    assert rec.added.empty and rec.retired.empty  # every row matched, nothing new


# ---------------------------------------------------------------------------
# One-sided nulls (Arrow three-valued `.ne()` footgun)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_one_sided_null_transitions_are_reported_changed() -> None:
    """`prev_col.ne(cur_col).fillna(False)` treats a one-sided null as
    *unchanged* -- Arrow's `<NA>.ne("AUT")` is `<NA>`, not `True`, and
    `fillna(False)` turns that into "no diff". `dotstat_code` is a real
    nullable contract column (multilaterals carry no dotstat code), so this
    exercises the actual failure mode rather than a synthetic one: a code
    gaining or losing a `dotstat_code` must land in `changed`, not slip
    through as if nothing happened.
    """
    snap1 = _snapshot(
        [
            {"code": "1", "dotstat_code": pd.NA},  # -> value: one-sided null
            {"code": "2", "dotstat_code": "BEL"},  # -> NA: one-sided null
            {"code": "3", "dotstat_code": pd.NA},  # -> NA: both-null, unchanged
            {"code": "4", "dotstat_code": "OLD"},  # -> different value
            {"code": "5", "dotstat_code": "SAME"},  # -> same value, unchanged
        ],
        fetched_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    rec1 = reconcile(previous=None, current=snap1)

    snap2 = _snapshot(
        [
            {"code": "1", "dotstat_code": "AUT"},
            {"code": "2", "dotstat_code": pd.NA},
            {"code": "3", "dotstat_code": pd.NA},
            {"code": "4", "dotstat_code": "NEW"},
            {"code": "5", "dotstat_code": "SAME"},
        ],
        fetched_at=datetime(2026, 2, 1, tzinfo=UTC),
    )
    rec2 = reconcile(previous=rec1.table, current=snap2)

    changed_codes = set(
        rec2.changed.loc[rec2.changed["column"] == "dotstat_code", "code"]
    )
    assert changed_codes == {"1", "2", "4"}
    assert rec2.is_unchanged is False

    def _changed_row(code: str) -> pd.Series:
        rows = rec2.changed[
            (rec2.changed["code"] == code) & (rec2.changed["column"] == "dotstat_code")
        ]
        assert len(rows) == 1
        return rows.iloc[0]

    row1 = _changed_row("1")
    assert pd.isna(row1["old_value"])
    assert row1["new_value"] == "AUT"

    row2 = _changed_row("2")
    assert row2["old_value"] == "BEL"
    assert pd.isna(row2["new_value"])

    # both-null (code 3) and same-value (code 5) must not appear in `changed`.
    assert "3" not in changed_codes
    assert "5" not in changed_codes

    # The table's live values reflect current regardless of `changed`.
    table_by_code = rec2.table.set_index("code")
    assert table_by_code.loc["1", "dotstat_code"] == "AUT"
    assert pd.isna(table_by_code.loc["2", "dotstat_code"])


@pytest.mark.unit
def test_both_null_column_is_unchanged() -> None:
    """The both-null case the pre-fix code already got right must stay right:
    `pd.NA` -> `pd.NA` on a nullable column is not a change."""
    snap1 = _snapshot(
        [{"code": "1", "dotstat_code": pd.NA}],
        fetched_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    rec1 = reconcile(previous=None, current=snap1)

    snap2 = _snapshot(
        [{"code": "1", "dotstat_code": pd.NA}],
        fetched_at=datetime(2026, 2, 1, tzinfo=UTC),
    )
    rec2 = reconcile(previous=rec1.table, current=snap2)

    assert rec2.changed.empty
    assert rec2.is_unchanged is True


# ---------------------------------------------------------------------------
# Partial fetch (rule 2 — partial codelists must not mass-retire skipped ones)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_partial_fetch_scopes_retirement_to_requested_codelists_synthetic() -> None:
    previous_snap = _snapshot(
        [{"code": "1", "codelist_id": "5"}, {"code": "1", "codelist_id": "13"}],
        fetched_at=datetime(2026, 1, 1, tzinfo=UTC),
        codelist_ids=("5", "13"),
    )
    rec1 = reconcile(previous=None, current=previous_snap)

    # This run only fetches codelist 5.
    current_snap = _snapshot(
        [{"code": "1", "codelist_id": "5"}],
        fetched_at=datetime(2026, 2, 1, tzinfo=UTC),
        codelist_ids=("5",),
    )
    rec2 = reconcile(previous=rec1.table, current=current_snap)

    cl13_row = rec2.table[rec2.table["codelist_id"] == "13"].iloc[0]
    assert cl13_row["presence"] == "current"  # untouched, not mass-retired
    assert cl13_row["last_seen"] == date(2026, 1, 1)  # untouched
    assert rec2.retired.empty


@pytest.mark.unit
def test_partial_fetch_scopes_retirement_to_requested_codelists_real_fixtures() -> None:
    raw = _load_fixture_raw()
    snap = parse_codelists(raw=raw, fetched_at=datetime(2026, 1, 1, tzinfo=UTC))
    rec1 = reconcile(previous=None, current=snap)

    snap_codelist5_only = parse_codelists(
        raw={"5": raw["5"]}, fetched_at=datetime(2026, 2, 1, tzinfo=UTC)
    )
    rec2 = reconcile(previous=rec1.table, current=snap_codelist5_only)

    cl13 = rec2.table[rec2.table["codelist_id"] == "13"]
    assert (cl13["presence"] == "current").all()
    assert (cl13["last_seen"] == date(2026, 1, 1)).all()
    assert rec2.retired.empty


# ---------------------------------------------------------------------------
# Coercion (rule 3 and float64 handling)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_duckdb_shaped_previous_coerces_cleanly() -> None:
    """`code` as int64, `codelist_id` as object — DuckDB's `.df()` shape for a no-null column."""
    snap1 = _snapshot(
        [{"code": "1"}, {"code": "2"}], fetched_at=datetime(2026, 1, 1, tzinfo=UTC)
    )
    rec1 = reconcile(previous=None, current=snap1)

    duckdb_shaped = rec1.table.copy()
    duckdb_shaped["code"] = duckdb_shaped["code"].astype("int64")
    duckdb_shaped["codelist_id"] = duckdb_shaped["codelist_id"].astype(object)

    snap2 = _snapshot(
        [{"code": "1"}, {"code": "2"}], fetched_at=datetime(2026, 2, 1, tzinfo=UTC)
    )
    rec2 = reconcile(previous=duckdb_shaped, current=snap2)

    assert len(rec2.table) == 2
    assert set(rec2.added["code"]) & set(rec2.retired["code"]) == set()
    assert rec2.added.empty and rec2.retired.empty
    for column in _CONTRACT_COLUMNS:
        expected = (
            "date32[day][pyarrow]" if column == "activation_date" else "string[pyarrow]"
        )
        # `str(dtype)` drops the pyarrow storage suffix ("string" instead of
        # "string[pyarrow]"), so compare dtype equality directly rather than
        # against `str()`/`repr()` -- their output format is not stable across
        # pandas versions (pandas 3 reprs this as `<StringDtype(na_value=<NA>)>`).
        assert rec2.table[column].dtype == expected, column


@pytest.mark.unit
def test_uncoercible_code_column_raises_naming_the_column() -> None:
    snap = _snapshot([{"code": "1"}], fetched_at=datetime(2026, 1, 1, tzinfo=UTC))
    rec1 = reconcile(previous=None, current=snap)

    lossy = rec1.table.copy()
    lossy["code"] = lossy["code"].astype("float64")
    lossy.loc[lossy.index[0], "code"] = (
        5.5  # not a whole number: cannot be losslessly re-keyed
    )

    with pytest.raises(CodelistValidationError) as excinfo:
        reconcile(previous=lossy, current=snap)
    assert "code" in excinfo.value.detail


@pytest.mark.unit
def test_float_kind_column_with_nan_does_not_corrupt_non_null_values() -> None:
    """Float-kind columns with NaN must be routed through an integer round-trip to avoid silent
    corruption: `[5.0, 13.0, np.nan].astype("string[pyarrow]")` yields `["5.0", "13.0", <NA>]`
    only with this protection. `dotstat_code` is a real nullable contract column (multilaterals
    carry no dotstat code), so this is the realistic shape — `code` itself cannot carry NaN
    because it is a non-nullable key column.
    """
    snap1 = _snapshot(
        [{"code": "5"}, {"code": "13", "dotstat_code": "MULTI"}],
        fetched_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    rec1 = reconcile(previous=None, current=snap1)

    duckdb_shaped = rec1.table.copy()
    # `code` as float64 with no actual null present — the realistic DuckDB nullable-integer
    # shape when *some other* column in the source table triggered the float64 dtype globally.
    duckdb_shaped["code"] = duckdb_shaped["code"].astype("float64")
    # `dotstat_code`: a genuinely nullable numeric-looking column, float64 with a real NaN.
    duckdb_shaped["dotstat_code"] = pd.array([float("nan"), 42.0], dtype="float64")

    snap2 = _snapshot(
        [{"code": "5"}, {"code": "13", "dotstat_code": "MULTI"}],
        fetched_at=datetime(2026, 2, 1, tzinfo=UTC),
    )
    rec2 = reconcile(previous=duckdb_shaped, current=snap2)

    assert set(rec2.table["code"]) == {"5", "13"}  # not {"5.0", "13.0"}
    assert len(rec2.table) == 2
    assert set(rec2.added["code"]) & set(rec2.retired["code"]) == set()


@pytest.mark.unit
def test_null_code_raises() -> None:
    """`code` is a key column and rule 3 requires it non-null; a genuinely missing code is not
    something reconcile can silently paper over (it is exactly the situation that makes a
    float64-typed `code` column arise from DuckDB in the first place)."""
    snap = _snapshot([{"code": "1"}], fetched_at=datetime(2026, 1, 1, tzinfo=UTC))
    rec1 = reconcile(previous=None, current=snap)

    broken = rec1.table.copy()
    broken["code"] = broken["code"].astype("float64")
    broken.loc[broken.index[0], "code"] = float("nan")

    with pytest.raises(CodelistValidationError) as excinfo:
        reconcile(previous=broken, current=snap)
    assert "code" in excinfo.value.detail


# ---------------------------------------------------------------------------
# Duplicates (rule 4)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_duplicate_key_in_previous_raises_listing_keys() -> None:
    snap = _snapshot([{"code": "1"}], fetched_at=datetime(2026, 1, 1, tzinfo=UTC))
    rec1 = reconcile(previous=None, current=snap)

    duplicated_previous = pd.concat(
        [rec1.table, rec1.table.iloc[[0]]], ignore_index=True
    )

    with pytest.raises(CodelistValidationError) as excinfo:
        reconcile(previous=duplicated_previous, current=snap)
    assert "5" in excinfo.value.detail
    assert "1" in excinfo.value.detail


@pytest.mark.unit
def test_duplicate_key_in_current_snapshot_raises() -> None:
    duplicated_snap = _snapshot(
        [{"code": "1"}, {"code": "1"}], fetched_at=datetime(2026, 1, 1, tzinfo=UTC)
    )
    with pytest.raises(CodelistValidationError):
        reconcile(previous=None, current=duplicated_snap)


# ---------------------------------------------------------------------------
# Consumer columns (rule 5)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_consumer_column_survives_and_is_na_for_new_rows() -> None:
    snap1 = _snapshot([{"code": "1"}], fetched_at=datetime(2026, 1, 1, tzinfo=UTC))
    rec1 = reconcile(previous=None, current=snap1)

    with_extra = rec1.table.copy()
    with_extra["loaded_at"] = pd.Timestamp("2026-01-01")

    snap2 = _snapshot(
        [{"code": "1"}, {"code": "2"}], fetched_at=datetime(2026, 2, 1, tzinfo=UTC)
    )
    rec2 = reconcile(previous=with_extra, current=snap2)

    assert "loaded_at" in rec2.table.columns
    existing_row = rec2.table[rec2.table["code"] == "1"].iloc[0]
    assert existing_row["loaded_at"] == pd.Timestamp("2026-01-01")
    new_row = rec2.table[rec2.table["code"] == "2"].iloc[0]
    assert pd.isna(new_row["loaded_at"])


@pytest.mark.unit
def test_reserved_lineage_column_with_out_of_domain_value_raises() -> None:
    snap = _snapshot([{"code": "1"}], fetched_at=datetime(2026, 1, 1, tzinfo=UTC))
    rec1 = reconcile(previous=None, current=snap)

    bad = rec1.table.copy()
    bad.loc[bad.index[0], "presence"] = "bogus"

    with pytest.raises(CodelistValidationError) as excinfo:
        reconcile(previous=bad, current=snap)
    assert "presence" in excinfo.value.detail


@pytest.mark.unit
def test_consumer_column_named_underscore_merge_survives() -> None:
    """A consumer column literally named `_merge` -- pandas' default merge
    indicator name -- must not make `reconcile` raise. Before the fix,
    `scoped_previous.merge(..., indicator=True, ...)` raised `ValueError:
    Cannot use name of an existing column for indicator column`.
    """
    snap1 = _snapshot([{"code": "1"}], fetched_at=datetime(2026, 1, 1, tzinfo=UTC))
    rec1 = reconcile(previous=None, current=snap1)

    with_extra = rec1.table.copy()
    with_extra["_merge"] = "consumer-value"

    snap2 = _snapshot(
        [{"code": "1"}, {"code": "2"}], fetched_at=datetime(2026, 2, 1, tzinfo=UTC)
    )
    rec2 = reconcile(previous=with_extra, current=snap2)

    assert "_merge" in rec2.table.columns
    existing_row = rec2.table[rec2.table["code"] == "1"].iloc[0]
    assert existing_row["_merge"] == "consumer-value"
    new_row = rec2.table[rec2.table["code"] == "2"].iloc[0]
    assert pd.isna(new_row["_merge"])


@pytest.mark.unit
def test_consumer_column_named_like_a_suffixed_compare_column_survives() -> None:
    """A consumer column named `label_prev` collides with the name
    `pd.DataFrame.merge` would generate for the suffixed `label` column.
    Before the fix this raised `MergeError: Passing 'suffixes' which cause
    duplicate columns {'label_prev'} is not allowed.`
    """
    snap1 = _snapshot([{"code": "1"}], fetched_at=datetime(2026, 1, 1, tzinfo=UTC))
    rec1 = reconcile(previous=None, current=snap1)

    with_extra = rec1.table.copy()
    with_extra["label_prev"] = "consumer-value"

    snap2 = _snapshot(
        [{"code": "1"}, {"code": "2"}], fetched_at=datetime(2026, 2, 1, tzinfo=UTC)
    )
    rec2 = reconcile(previous=with_extra, current=snap2)

    assert "label_prev" in rec2.table.columns
    existing_row = rec2.table[rec2.table["code"] == "1"].iloc[0]
    assert existing_row["label_prev"] == "consumer-value"
    new_row = rec2.table[rec2.table["code"] == "2"].iloc[0]
    assert pd.isna(new_row["label_prev"])


# ---------------------------------------------------------------------------
# source_status vocabulary (rule 5 passthrough, not a domain gate)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_reconcile_output_with_unknown_status_round_trips_as_next_previous() -> None:
    """`parse_codelists` deliberately passes an unrecognised OECD status
    through rather than dropping it, and records it in
    `CodelistSnapshot.unknown_statuses`. `reconcile` copies `status` into
    `source_status`, so on the next run that value comes back as
    `previous.source_status` -- and it must be accepted, not rejected as
    out-of-domain. Before the fix, the second `reconcile` call raised
    `CodelistValidationError: previous.source_status has value(s) outside
    the documented status domain`.
    """
    snap1 = _snapshot(
        [{"code": "1", "status": "suspended"}],
        fetched_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    rec1 = reconcile(previous=None, current=snap1)
    assert rec1.table.loc[0, "source_status"] == "suspended"

    snap2 = _snapshot(
        [{"code": "1", "status": "suspended"}],
        fetched_at=datetime(2026, 2, 1, tzinfo=UTC),
    )
    rec2 = reconcile(previous=rec1.table, current=snap2)  # must not raise

    assert rec2.table.loc[0, "source_status"] == "suspended"
    assert rec2.is_unchanged is True


# ---------------------------------------------------------------------------
# Reappearance (rule 6)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_reappearing_code_is_changed_not_added() -> None:
    snap1 = _snapshot(
        [{"code": "1"}, {"code": "2"}], fetched_at=datetime(2026, 1, 1, tzinfo=UTC)
    )
    rec1 = reconcile(previous=None, current=snap1)

    snap2 = _snapshot([{"code": "1"}], fetched_at=datetime(2026, 2, 1, tzinfo=UTC))
    rec2 = reconcile(previous=rec1.table, current=snap2)
    assert list(rec2.retired["code"]) == ["2"]

    snap3 = _snapshot(
        [{"code": "1"}, {"code": "2"}], fetched_at=datetime(2026, 3, 1, tzinfo=UTC)
    )
    rec3 = reconcile(previous=rec2.table, current=snap3)

    assert "2" not in set(rec3.added["code"])  # rule 6: not an addition
    reappearance_rows = rec3.changed[
        (rec3.changed["code"] == "2") & (rec3.changed["column"] == "presence")
    ]
    assert len(reappearance_rows) == 1
    assert reappearance_rows.iloc[0]["old_value"] == "retired"
    assert reappearance_rows.iloc[0]["new_value"] == "current"

    row = rec3.table[rec3.table["code"] == "2"].iloc[0]
    assert row["presence"] == "current"
    assert row["first_seen"] == date(
        2026, 1, 1
    )  # unchanged, kept from its original appearance


# ---------------------------------------------------------------------------
# observed_at (rule 7)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_observed_at_defaults_to_fetched_at_with_no_second_clock_read() -> None:
    """Dependency-free clock test (no freezegun — just set `fetched_at` to a date that is
    plainly not today; if the implementation reads the real clock, `last_seen` comes back as
    today and this assertion fails, making the bug obvious).
    """
    snap = _snapshot([{"code": "1"}], fetched_at=datetime(2020, 1, 1, tzinfo=UTC))
    rec = reconcile(previous=None, current=snap)
    assert (rec.table["last_seen"] == date(2020, 1, 1)).all()
    assert (rec.table["first_seen"] == date(2020, 1, 1)).all()


@pytest.mark.unit
def test_observed_at_earlier_than_a_last_seen_in_previous_raises() -> None:
    snap1 = _snapshot([{"code": "1"}], fetched_at=datetime(2026, 6, 1, tzinfo=UTC))
    rec1 = reconcile(previous=None, current=snap1)

    snap2 = _snapshot([{"code": "1"}], fetched_at=datetime(2026, 6, 1, tzinfo=UTC))
    with pytest.raises(CodelistValidationError) as excinfo:
        reconcile(previous=rec1.table, current=snap2, observed_at=date(2020, 1, 1))
    assert "observed_at" in excinfo.value.detail


# ---------------------------------------------------------------------------
# is_unchanged precision
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_is_unchanged_stays_true_when_only_last_seen_advances() -> None:
    snap1 = _snapshot([{"code": "1"}], fetched_at=datetime(2026, 1, 1, tzinfo=UTC))
    rec1 = reconcile(previous=None, current=snap1)

    snap2 = _snapshot([{"code": "1"}], fetched_at=datetime(2026, 6, 1, tzinfo=UTC))
    rec2 = reconcile(previous=rec1.table, current=snap2)

    assert rec1.table.loc[0, "last_seen"] != rec2.table.loc[0, "last_seen"]
    assert rec2.is_unchanged is True


# ---------------------------------------------------------------------------
# The superset property
# ---------------------------------------------------------------------------


def _random_snapshot(
    rng: random.Random, *, universe: range, fetched_at: datetime
) -> CodelistSnapshot:
    codes = rng.sample(universe, k=rng.randint(1, len(universe)))
    rows = [
        {"code": str(code), "label": f"Label {code} v{rng.randint(0, 9)}"}
        for code in codes
    ]
    return _snapshot(rows, fetched_at=fetched_at)


@pytest.mark.unit
def test_superset_property_hand_rolled_generator() -> None:
    """Rule 1 and rule 8's "CREATE OR REPLACE stays safe" promise: whatever key set `previous`
    held going in, `table` holds coming out — over many generated fetches, not just one
    hand-picked example. Seeded with `random.Random(0)` for determinism.
    """
    rng = random.Random(0)
    universe = range(1, 30)
    previous_table: pd.DataFrame | None = None

    for round_index in range(25):
        # Strictly increasing across all 25 rounds -- observed_at must never move
        # backwards relative to a last_seen already in previous (rule 7).
        fetched_at = datetime(2026, 1, 1, tzinfo=UTC) + pd.Timedelta(days=round_index)
        snap = _random_snapshot(rng, universe=universe, fetched_at=fetched_at)
        rec = reconcile(previous=previous_table, current=snap)

        if previous_table is not None:
            prev_keys = set(
                zip(
                    previous_table["codelist_id"],
                    previous_table["code"],
                    previous_table["activation_date"],
                    strict=True,
                )
            )
            now_keys = set(
                zip(
                    rec.table["codelist_id"],
                    rec.table["code"],
                    rec.table["activation_date"],
                    strict=True,
                )
            )
            assert prev_keys <= now_keys, prev_keys - now_keys

        previous_table = rec.table


@pytest.mark.unit
def test_superset_property_holds_on_real_fixtures() -> None:
    """The same superset property, spot-checked against the real 577-row fixture frame rather
    than a synthetic one. Catches the subtle bug where a `groupby`/`merge` on nullable key
    columns drops rows with null `activation_date` from the output.
    """
    raw = _load_fixture_raw()
    snap1 = parse_codelists(raw=raw, fetched_at=datetime(2026, 1, 1, tzinfo=UTC))
    rec1 = reconcile(previous=None, current=snap1)
    assert len(rec1.table) == 577

    null_activation_date_rows = rec1.table[rec1.table["activation_date"].isna()]
    assert (
        len(null_activation_date_rows) == 214
    )  # 213 (codelist 5) + code 999 (codelist 13)

    snap2 = parse_codelists(raw=raw, fetched_at=datetime(2026, 2, 1, tzinfo=UTC))
    rec2 = reconcile(previous=rec1.table, current=snap2)

    prev_keys = set(
        zip(
            rec1.table["codelist_id"],
            rec1.table["code"],
            rec1.table["activation_date"],
            strict=True,
        )
    )
    now_keys = set(
        zip(
            rec2.table["codelist_id"],
            rec2.table["code"],
            rec2.table["activation_date"],
            strict=True,
        )
    )
    assert prev_keys <= now_keys
    assert len(rec2.table) == 577  # nothing added, nothing dropped, nothing retired
    assert rec2.is_unchanged is True
