"""Merge a previous codelist table with a fresh snapshot, never deleting.

A pure function that hands a warehouse consumer a never-delete guarantee:
the output is a superset of the input by ``(codelist_id, code, activation_date)``.
See ``reconcile``'s docstring for the ten rules this module implements. Every
rule is a data-corruption bug if it regresses, so the comments below are
deliberately verbose about *why*, not just *what*.

Two pandas footguns sit directly in this module's path,
verified against pandas 2.3.3 and 3.0.5, pyarrow 22.0.0:

- Coercing a float-kind column straight to ``string[pyarrow]`` appends a
  spurious ``".0"`` to every non-null value (``5.0`` instead of ``5``),
  silently, with no exception. ``code`` and ``codelist_id`` are join keys,
  so this reproduces the exact "every row is simultaneously new and
  retired" failure rule 3 exists to prevent. See ``_coerce_string_column``.
- Scalar ``pd.NA != pd.NA`` raises ``TypeError``, so a row-wise loop doing
  ``if old != new`` crashes on the common case of two nullable columns
  both being null. Every comparison here is the vectorized
  ``old_col.ne(new_col)`` instead, which returns ``<NA>`` where both sides
  are null and lets pandas' NA-aware boolean indexing exclude those rows.

``activation_date`` is the third key column and is nullable: all 213
codelist-5 rows key on a null ``activation_date``, as does codelist-13
code ``999``. ``pd.DataFrame.merge`` and
``pd.DataFrame.duplicated`` both match/detect ``pd.NA`` correctly on this
repo's pinned stack (verified interactively), so this module builds every
key operation on ``merge`` and ``duplicated`` rather than ``groupby``,
which drops NA-keyed rows unless called with ``dropna=False``. There is no
``groupby`` on the key anywhere in this file.
"""

from __future__ import annotations

from datetime import UTC, date

import pandas as pd

from oda_reader.codelists._parse import _CONTRACT_COLUMNS
from oda_reader.codelists._types import (
    LINEAGE_COLUMNS,
    PRESENCE_DOMAIN,
    CodelistSnapshot,
    Reconciliation,
)
from oda_reader.exceptions import CodelistValidationError

# The reconciliation key. `activation_date` is the nullable third component
# -- see the module docstring.
_KEY: tuple[str, ...] = ("codelist_id", "code", "activation_date")

# The seven contract columns that can differ between an unchanged code's
# previous and current row. `codelist_id`, `code` and `activation_date`
# are the key and can't "change" without becoming a different row.
_COMPARE_COLUMNS: tuple[str, ...] = tuple(
    column for column in _CONTRACT_COLUMNS if column not in _KEY
)

# `changed`'s column set. `column`/`old_value`/`new_value` are always
# strings here: every entry in `_COMPARE_COLUMNS` is `string[pyarrow]` on
# the contract, and the one non-column-diff case (rule 6's presence
# transition) reports the two `PRESENCE_DOMAIN` strings.
_CHANGED_COLUMNS: tuple[str, ...] = (
    "codelist_id",
    "code",
    "activation_date",
    "column",
    "old_value",
    "new_value",
    "observed_at",
)


def _coerce_string_column(series: pd.Series, *, column: str) -> pd.Series:
    """Coerce *series* to ``string[pyarrow]``, guarding the float-coercion footgun.

    DuckDB's ``.df()`` returns ``int64`` for a column with no nulls, but a
    nullable integer column comes back ``float64`` -- and
    ``pd.Series([5.0, 13.0, np.nan]).astype("string[pyarrow]")`` yields
    ``["5.0", "13.0", <NA>]``: the null coerces fine, but every non-null
    value picks up a trailing ``.0``. `.astype()` does not raise, so this
    fails silently. A float-kind column is therefore round-tripped through
    a lossless nullable-integer cast first; a value that isn't a whole
    number raises rather than silently truncating.
    """
    if pd.api.types.is_float_dtype(series):
        as_float = series.astype("Float64")
        as_int = as_float.round().astype("Int64")
        lossy = as_float.notna() & (as_int.astype("Float64") != as_float)
        if lossy.any():
            raise CodelistValidationError(
                detail=(
                    f"previous.{column} cannot be coerced to a codelist key: "
                    "it holds non-integer float value(s)"
                )
            )
        return as_int.astype("string[pyarrow]")
    try:
        return series.astype("string[pyarrow]")
    except (ValueError, TypeError) as exc:
        raise CodelistValidationError(
            detail=f"previous.{column} could not be coerced to string[pyarrow]: {exc}"
        ) from exc


def _coerce_key_column(series: pd.Series, *, column: str) -> pd.Series:
    """Coerce *series* to a non-null, whitespace-stripped ``string[pyarrow]`` key.

    Rule 3: ``codelist_id`` and ``code`` are normalised to non-null strings
    with surrounding whitespace stripped -- no other normalisation.
    ``"05"`` and ``"5"`` stay different codes.
    """
    coerced = _coerce_string_column(series, column=column)
    stripped = coerced.str.strip()
    if stripped.isna().any():
        raise CodelistValidationError(
            detail=(
                f"previous.{column} has null value(s); it is part of the "
                "reconciliation key and must not be null"
            )
        )
    return stripped


def _coerce_date_column(series: pd.Series, *, column: str) -> pd.Series:
    """Coerce *series* to ``date32[pyarrow]``. Nulls stay null (rule 3)."""
    try:
        return series.astype("date32[pyarrow]")
    except (ValueError, TypeError) as exc:
        raise CodelistValidationError(
            detail=f"previous.{column} could not be coerced to date32[pyarrow]: {exc}"
        ) from exc


def _coerce_previous(previous: pd.DataFrame) -> pd.DataFrame:
    """Coerce *previous*'s contract columns to contract dtypes; backfill lineage.

    Rule 3: ``previous`` will almost never arrive with the dtypes we
    produced it with, so every contract column is coerced on entry, with
    the key columns (``codelist_id``, ``code``, ``activation_date``)
    getting the strict non-null / lossless treatment.

    Rule 5: the four lineage column names are reserved. A column present
    with an incompatible dtype raises; ``presence`` additionally raises on
    an out-of-domain value, because ``presence`` is vocabulary reconcile
    itself owns end to end. ``source_status`` does not get that domain
    check -- see the note below. A column *absent* is backfilled:
    ``first_seen`` and ``last_seen`` default to ``pd.NA`` (we never lie
    about what we do not know), ``source_status`` defaults to ``pd.NA``,
    and ``presence`` defaults to ``"current"`` -- absent contrary evidence,
    a code already in the consumer's table is presumed current.

    ``source_status`` is only checked for shape (coercible to
    ``string[pyarrow]``), not against ``STATUS_DOMAIN``.
    ``parse_codelists`` deliberately passes an unrecognised OECD status
    through rather than dropping it -- "dropping data we do not recognise
    is worse than passing it on" -- and ``reconcile`` copies ``status``
    straight into ``source_status``. A domain check here would make
    ``reconcile``'s own output unable to round-trip as its next
    ``previous`` the moment OECD emits a status this library does not yet
    know about, which contradicts the passthrough guarantee instead of
    enforcing anything upstream ever promised. ``STATUS_DOMAIN`` still
    documents OECD's known vocabulary and still drives
    ``CodelistSnapshot.unknown_statuses``; it is just not a gate here.
    """
    missing = [column for column in _CONTRACT_COLUMNS if column not in previous.columns]
    if missing:
        raise CodelistValidationError(
            detail=f"previous is missing contract column(s): {', '.join(missing)}"
        )

    result = previous.copy()
    result["codelist_id"] = _coerce_key_column(
        result["codelist_id"], column="codelist_id"
    )
    result["code"] = _coerce_key_column(result["code"], column="code")
    result["activation_date"] = _coerce_date_column(
        result["activation_date"], column="activation_date"
    )
    for column in _COMPARE_COLUMNS:
        result[column] = _coerce_string_column(result[column], column=column)

    # Backfill lineage columns with defaults or strip as needed

    n = len(result)
    if "first_seen" in result.columns:
        result["first_seen"] = _coerce_date_column(
            result["first_seen"], column="first_seen"
        )
    else:
        result["first_seen"] = pd.array([pd.NA] * n, dtype="date32[pyarrow]")

    if "last_seen" in result.columns:
        result["last_seen"] = _coerce_date_column(
            result["last_seen"], column="last_seen"
        )
    else:
        result["last_seen"] = pd.array([pd.NA] * n, dtype="date32[pyarrow]")

    if "source_status" in result.columns:
        # No STATUS_DOMAIN check: see the docstring note above. Shape only.
        result["source_status"] = _coerce_string_column(
            result["source_status"], column="source_status"
        )
    else:
        result["source_status"] = pd.array([pd.NA] * n, dtype="string[pyarrow]")

    if "presence" in result.columns:
        result["presence"] = _coerce_string_column(
            result["presence"], column="presence"
        )
        bad = ~result["presence"].isin(PRESENCE_DOMAIN)
        if bad.any():
            bad_values = sorted(
                str(value) for value in result.loc[bad, "presence"].fillna("<NA>")
            )
            raise CodelistValidationError(
                detail=(
                    f"previous.presence has value(s) outside {PRESENCE_DOMAIN}: {bad_values}"
                )
            )
    else:
        result["presence"] = pd.array(["current"] * n, dtype="string[pyarrow]")

    return result


def _assert_area_contract(snapshot: CodelistSnapshot) -> None:
    """Reject a snapshot that is not the area contract's.

    ``reconcile`` hardcodes the area key (``_KEY`` above) and the area
    contract's column set (``_CONTRACT_COLUMNS``, imported from
    ``_parse.py``). Neither of the other two contracts' ``CodelistSnapshot``
    shapes fits that key: the category contract's (from
    ``fetch_code_categories`` / ``parse_code_categories``) carries a
    different eleven-column frame keyed on five columns, not three, and the
    agency contract's (from ``fetch_provider_agencies`` /
    ``parse_provider_agencies``) adds ``donor_code`` to the key, where
    ``activation_date`` is null on every row published so far --
    reconciling either one here would silently mis-key every row rather
    than fail loudly.

    Two checks, in order. ``contract`` is the primary signal: it is set by
    whichever parse function built the snapshot and says what the caller
    meant, so it catches a non-area snapshot even if its frame has been
    edited into area-looking columns. The column-tuple comparison stays as a
    second line of defence for a snapshot hand-built from stored ``raw``,
    where ``contract`` is whatever the caller declared and the frame may not
    match it. Exact tuple equality is precise here because every
    ``_build_frame`` implementation always emits its contract's columns in
    the documented order.
    """
    if snapshot.contract != "area":
        raise CodelistValidationError(
            detail=(
                f"reconcile only supports the area contract (fetch_codelists / "
                f"parse_codelists); got a snapshot whose contract is "
                f"{snapshot.contract!r}."
            )
        )
    frame = snapshot.frame
    if tuple(frame.columns) != _CONTRACT_COLUMNS:
        raise CodelistValidationError(
            detail=(
                "reconcile only supports the area contract's ten-column frame "
                f"{_CONTRACT_COLUMNS!r}; got columns {tuple(frame.columns)!r}. "
                "Neither the category contract (fetch_code_categories / "
                "parse_code_categories) nor the agency contract "
                "(fetch_provider_agencies / parse_provider_agencies) is "
                "supported by reconcile."
            )
        )


def _assert_unique_key(frame: pd.DataFrame, *, source: str) -> None:
    """Rule 4: a duplicate ``(codelist_id, code, activation_date)`` key is an error.

    ``DataFrame.duplicated`` -- unlike ``groupby`` -- matches ``pd.NA`` keys
    correctly with no special-casing needed (verified interactively against
    this repo's pinned pandas), so it is safe to use directly here.
    """
    duplicated = frame.duplicated(subset=list(_KEY), keep=False)
    if not duplicated.any():
        return
    offending = frame.loc[duplicated, list(_KEY)].drop_duplicates()
    keys = sorted(
        (
            row.codelist_id,
            row.code,
            None if pd.isna(row.activation_date) else row.activation_date.isoformat(),
        )
        for row in offending.itertuples(index=False)
    )
    raise CodelistValidationError(
        detail=f"{source} has duplicate (codelist_id, code, activation_date) key(s): {keys}"
    )


def _resolve_observed_at(current: CodelistSnapshot, observed_at: date | None) -> date:
    """Rule 7: default to ``current.fetched_at`` as a UTC date, not a second clock read."""
    if observed_at is not None:
        return observed_at
    return current.fetched_at.astimezone(UTC).date()


def _validate_observed_at_order(*, observed_at: date, previous: pd.DataFrame) -> None:
    """Rule 7: an out-of-order backfill replay is validated, not silently accepted."""
    last_seen = previous["last_seen"]
    stale = last_seen.notna() & (last_seen > observed_at)
    if stale.any():
        raise CodelistValidationError(
            detail=(
                f"observed_at={observed_at.isoformat()} is earlier than a last_seen "
                f"already present in previous (max last_seen={last_seen.max()})"
            )
        )


def _changed_rows(
    *,
    key_frame: pd.DataFrame,
    column: str,
    old_value: pd.Series | str,
    new_value: pd.Series | str,
    observed_at: date,
) -> pd.DataFrame:
    """One block of ``changed`` rows, keyed by *key_frame*, all attributed to *column*.

    *old_value*/*new_value* are either a ``pd.Series`` aligned to
    *key_frame*'s index (the per-column-diff case) or a scalar broadcast to
    every row (rule 6's presence-reappearance case). Dtype is enforced
    explicitly here rather than left to inference, so every block
    concatenates cleanly regardless of which case produced it.
    """
    n = len(key_frame)
    codelist_id = key_frame["codelist_id"].reset_index(drop=True)
    code = key_frame["code"].reset_index(drop=True)
    activation_date = key_frame["activation_date"].reset_index(drop=True)

    def _as_string_column(value: pd.Series | str) -> pd.Series:
        if isinstance(value, pd.Series):
            return value.reset_index(drop=True).astype("string[pyarrow]")
        return pd.Series(pd.array([value] * n, dtype="string[pyarrow]"))

    frame = pd.DataFrame(
        {
            "codelist_id": codelist_id,
            "code": code,
            "activation_date": activation_date,
            "column": pd.array([column] * n, dtype="string[pyarrow]"),
            "old_value": _as_string_column(old_value),
            "new_value": _as_string_column(new_value),
            "observed_at": pd.array([observed_at] * n, dtype="date32[pyarrow]"),
        }
    )
    return frame[list(_CHANGED_COLUMNS)]


def _empty_changed_frame() -> pd.DataFrame:
    dtypes = {
        "codelist_id": "string[pyarrow]",
        "code": "string[pyarrow]",
        "activation_date": "date32[pyarrow]",
        "column": "string[pyarrow]",
        "old_value": "string[pyarrow]",
        "new_value": "string[pyarrow]",
        "observed_at": "date32[pyarrow]",
    }
    return pd.DataFrame(
        {name: pd.array([], dtype=dtypes[name]) for name in _CHANGED_COLUMNS}
    )


def _reconcile_first_run(
    *, current: CodelistSnapshot, observed_at: date
) -> Reconciliation:
    """``previous=None``: every row is new, nothing to compare against (rule 9)."""
    table = current.frame.copy()
    n = len(table)
    table["first_seen"] = pd.array([observed_at] * n, dtype="date32[pyarrow]")
    table["last_seen"] = pd.array([observed_at] * n, dtype="date32[pyarrow]")
    table["source_status"] = table["status"]
    table["presence"] = pd.array(["current"] * n, dtype="string[pyarrow]")
    table = table.reset_index(drop=True)

    added = table.copy()
    retired = table.iloc[0:0].copy()
    changed = _empty_changed_frame()
    return Reconciliation(
        table=table,
        added=added,
        retired=retired,
        changed=changed,
        is_unchanged=added.empty and retired.empty and changed.empty,
    )


def _merge_names(
    *, left_columns: pd.Index, right_columns: pd.Index
) -> tuple[str, str, str]:
    """Pick an ``indicator`` column name and ``(prev, cur)`` suffixes that
    ``pd.DataFrame.merge`` can use on *left_columns* / *right_columns*
    without producing a duplicate column name in its output.

    Rule 5: any non-lineage column in *previous* survives reconciliation --
    including one a consumer happens to have named ``_merge`` (pandas'
    default indicator name; ``merge`` raises rather than accept it) or
    ``<contract_column>_prev`` / ``_cur`` (the exact names ``merge`` would
    otherwise generate for a suffixed compare column, producing a duplicate
    and a ``MergeError``). Both names are grown with a trailing underscore
    until they are absent from both frames' columns entirely, so a
    once-in-a-while consumer column name never breaks reconciliation.
    """
    all_columns = set(left_columns) | set(right_columns)

    indicator = "_merge"
    while indicator in all_columns:
        indicator += "_"

    suffix_prev, suffix_cur = "_prev", "_cur"
    while any(
        f"{column}{suffix_prev}" in all_columns
        or f"{column}{suffix_cur}" in all_columns
        for column in _COMPARE_COLUMNS
    ):
        suffix_prev += "_"
        suffix_cur += "_"

    return indicator, suffix_prev, suffix_cur


def _reconcile_both(
    both: pd.DataFrame,
    *,
    observed_at: date,
    indicator: str,
    suffix_prev: str,
    suffix_cur: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Rows present in both scoped previous and current: refresh, detect changes.

    Contract columns take current's fresh values. ``last_seen`` advances to
    *observed_at*. ``first_seen`` propagates untouched (rule 10 -- a
    ``pd.NA`` first_seen stays ``pd.NA`` here; only a genuinely new row
    (`_reconcile_right_only`) ever sets it). ``presence`` becomes
    ``"current"`` unconditionally; if it *was* ``"retired"``, that is
    rule 6's reappearance and is reported in ``changed``, never ``added``.

    *indicator*, *suffix_prev* and *suffix_cur* are collision-safe merge names
    picked to avoid duplicate column names on the merge operation.
    """
    result = both.copy()

    changed_parts: list[pd.DataFrame] = []
    for column in _COMPARE_COLUMNS:
        prev_col = both[f"{column}{suffix_prev}"]
        cur_col = both[f"{column}{suffix_cur}"]
        # `.eq()` on string[pyarrow] returns bool[pyarrow] with <NA> wherever
        # either side is null -- Arrow three-valued logic, not just the
        # both-null case. `.fillna(False)` on the *equality* turns every
        # null-involving pair into "not equal", which is right when exactly
        # one side is null (that's a real change: the code gained or lost a
        # value) but wrong when both sides are null (nothing changed, and
        # `NA == NA` is not a difference to report). `both_na` carves that
        # second case back out. Do not `.fillna(False)` `.ne()` directly --
        # that classifies a one-sided null as unchanged, which silently
        # suppresses the alert `changed` exists to raise.
        both_na = prev_col.isna() & cur_col.isna()
        diff = ~(prev_col.eq(cur_col).fillna(False) | both_na)
        if diff.any():
            changed_parts.append(
                _changed_rows(
                    key_frame=both.loc[diff],
                    column=column,
                    old_value=prev_col[diff],
                    new_value=cur_col[diff],
                    observed_at=observed_at,
                )
            )
        result[column] = cur_col

    reappeared = result["presence"].eq("retired")
    if reappeared.any():
        changed_parts.append(
            _changed_rows(
                key_frame=both.loc[reappeared],
                column="presence",
                old_value="retired",
                new_value="current",
                observed_at=observed_at,
            )
        )

    result["source_status"] = result["status"]
    result["presence"] = pd.array(["current"] * len(result), dtype="string[pyarrow]")
    result["last_seen"] = pd.array([observed_at] * len(result), dtype="date32[pyarrow]")
    # first_seen: left as-is, already carried over from `both`'s (unsuffixed) previous value.

    drop_columns = [f"{c}{suffix_prev}" for c in _COMPARE_COLUMNS]
    drop_columns += [f"{c}{suffix_cur}" for c in _COMPARE_COLUMNS]
    drop_columns.append(indicator)
    result = result.drop(columns=drop_columns)

    changed = (
        pd.concat(changed_parts, ignore_index=True)
        if changed_parts
        else _empty_changed_frame()
    )
    return result, changed


def _reconcile_left_only(
    left_only: pd.DataFrame, *, indicator: str, suffix_prev: str, suffix_cur: str
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Rows in scoped previous, absent from current this run: retire or stay retired.

    Rule 1: values and ``last_seen`` stay exactly as they were in
    *previous* -- "last known values intact". Only ``presence`` moves, and
    only for rows that were ``"current"``; a row already ``"retired"`` is
    untouched (it is not *newly* retired this run, so it does not belong
    in the ``retired`` output -- see ``Reconciliation.retired``).
    """
    rename = {f"{c}{suffix_prev}": c for c in _COMPARE_COLUMNS}
    drop_columns = [f"{c}{suffix_cur}" for c in _COMPARE_COLUMNS] + [indicator]
    result = left_only.drop(columns=drop_columns).rename(columns=rename)

    was_current = result["presence"].eq("current")
    result["presence"] = pd.array(["retired"] * len(result), dtype="string[pyarrow]")
    newly_retired = result.loc[was_current]
    return result, newly_retired


def _reconcile_right_only(
    right_only: pd.DataFrame,
    *,
    observed_at: date,
    indicator: str,
    suffix_prev: str,
    suffix_cur: str,
) -> pd.DataFrame:
    """Rows in current, absent from scoped previous entirely: genuinely new."""
    rename = {f"{c}{suffix_cur}": c for c in _COMPARE_COLUMNS}
    drop_columns = [f"{c}{suffix_prev}" for c in _COMPARE_COLUMNS] + [indicator]
    result = right_only.drop(columns=drop_columns).rename(columns=rename)

    n = len(result)
    result["first_seen"] = pd.array([observed_at] * n, dtype="date32[pyarrow]")
    result["last_seen"] = pd.array([observed_at] * n, dtype="date32[pyarrow]")
    result["source_status"] = result["status"]
    result["presence"] = pd.array(["current"] * n, dtype="string[pyarrow]")
    return result


def _reconcile_with_previous(
    *, previous: pd.DataFrame, current: CodelistSnapshot, observed_at: date
) -> Reconciliation:
    current_ids = set(current.codelist_ids)
    # Rule 2: retirement is scoped to current.codelist_ids. A previous row
    # whose codelist_id was not requested this run passes through
    # untouched -- absent-because-not-requested is not retirement.
    in_scope = previous["codelist_id"].isin(current_ids)
    scoped_previous = previous.loc[in_scope]
    unscoped_previous = previous.loc[~in_scope]

    indicator, suffix_prev, suffix_cur = _merge_names(
        left_columns=scoped_previous.columns, right_columns=current.frame.columns
    )
    merged = scoped_previous.merge(
        current.frame,
        on=list(_KEY),
        how="outer",
        indicator=indicator,
        suffixes=(suffix_prev, suffix_cur),
    )

    both_rows, both_changed = _reconcile_both(
        merged.loc[merged[indicator] == "both"],
        observed_at=observed_at,
        indicator=indicator,
        suffix_prev=suffix_prev,
        suffix_cur=suffix_cur,
    )
    left_rows, newly_retired = _reconcile_left_only(
        merged.loc[merged[indicator] == "left_only"],
        indicator=indicator,
        suffix_prev=suffix_prev,
        suffix_cur=suffix_cur,
    )
    right_rows = _reconcile_right_only(
        merged.loc[merged[indicator] == "right_only"],
        observed_at=observed_at,
        indicator=indicator,
        suffix_prev=suffix_prev,
        suffix_cur=suffix_cur,
    )

    extra_columns = [
        column
        for column in previous.columns
        if column not in _CONTRACT_COLUMNS and column not in LINEAGE_COLUMNS
    ]
    column_order = list(_CONTRACT_COLUMNS) + list(LINEAGE_COLUMNS) + extra_columns

    table = pd.concat(
        [unscoped_previous, left_rows, both_rows, right_rows],
        ignore_index=True,
    )[column_order]

    added = right_rows[column_order].reset_index(drop=True)
    retired = newly_retired[column_order].reset_index(drop=True)
    # `_reconcile_both` already returns `_CHANGED_COLUMNS`-shaped output,
    # whether empty or not -- nothing left to reorder here.
    changed = both_changed

    return Reconciliation(
        table=table,
        added=added,
        retired=retired,
        changed=changed,
        is_unchanged=added.empty and retired.empty and changed.empty,
    )


def reconcile(
    *,
    previous: pd.DataFrame | None,
    current: CodelistSnapshot,
    observed_at: date | None = None,
) -> Reconciliation:
    """Merge a previous codelist table with a fresh snapshot, never deleting.

    A pure function: no I/O, no state, deterministic given its inputs.
    ``oda_reader.codelists`` never tells you a code stopped existing. It
    tells you a code stopped being listed. Give us your previous table and
    the table we give back is a superset of it by ``(codelist_id, code,
    activation_date)``.

    Args:
        previous: The consumer's existing table, or ``None`` on first run.
            May be a plain snapshot frame with no lineage columns; they
            will be backfilled. Contract columns are coerced to contract
            dtypes on entry.
        current: A snapshot from ``fetch_codelists`` or ``parse_codelists``.
        observed_at: Backfill only. Defaults to ``current.fetched_at`` as a
            UTC date, with no second call to the clock.

    Returns:
        A ``Reconciliation`` whose ``.table`` is a superset of *previous*
        by ``(codelist_id, code, activation_date)``. No input row is ever
        dropped.

    Raises:
        CodelistValidationError: *current*'s frame is not the area
            contract's ten-column shape (e.g. a category-contract snapshot
            from ``fetch_code_categories`` / ``parse_code_categories``, or
            an agency-contract snapshot from ``fetch_provider_agencies`` /
            ``parse_provider_agencies`` -- ``reconcile`` hardcodes the area
            key and does not support either one). Also raised for:
            duplicate keys in *previous* or in *current*'s
            frame, a *previous* contract or key column that cannot be
            coerced to its contract dtype without loss, a reserved lineage
            column present with an incompatible type, a ``presence`` value
            outside ``{"current", "retired"}``, or an *observed_at* earlier
            than a ``last_seen`` already present in *previous*.
            ``source_status`` is exempt from a domain check -- it
            round-trips whatever ``parse_codelists`` passed through,
            including a status this library does not yet recognise.

    Notes:
        Retirement is scoped to current.codelist_ids. A previous row whose codelist_id
        was not among the ids fetched this run passes through untouched — same presence,
        same last_seen. Absent-because-not-requested is not retirement.

        A code that was retired and reappears returns to presence="current", keeps its
        original first_seen, and is reported in `changed` with column="presence".
        It does NOT appear in `added`.
    """
    _assert_area_contract(current)
    resolved_observed_at = _resolve_observed_at(current, observed_at)
    _assert_unique_key(current.frame, source="current snapshot")

    if previous is None:
        return _reconcile_first_run(current=current, observed_at=resolved_observed_at)

    coerced_previous = _coerce_previous(previous)
    _assert_unique_key(coerced_previous, source="previous")
    _validate_observed_at_order(
        observed_at=resolved_observed_at, previous=coerced_previous
    )

    return _reconcile_with_previous(
        previous=coerced_previous, current=current, observed_at=resolved_observed_at
    )
