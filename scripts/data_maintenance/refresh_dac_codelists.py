"""Refresh OECD DAC area codelist snapshots.

Fetches two OECD area codelists (Providers=5, Recipients=13) via the
two-step ASPX/VIEWSTATE POST handshake, projects each into the existing
{dac_numeric_code: dotstat_code} shape, prints a unified diff against
the committed mappings/*.json, and never auto-writes.

Usage:
    # diff-only (default):
    uv run python -m scripts.data_maintenance.refresh_dac_codelists

    # capture raw fixtures for offline testing (writes and exits):
    uv run python -m scripts.data_maintenance.refresh_dac_codelists \\
        --capture-fixtures tests/fixtures/oecd

    # apply proposed changes:
    uv run python -m scripts.data_maintenance.refresh_dac_codelists --write

    # print the withdrawn-record conflict/orphan/new-candidate findings,
    # writing nothing:
    uv run python -m scripts.data_maintenance.refresh_dac_codelists --audit
"""

from __future__ import annotations

import argparse
import datetime
import json
import sys
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from oda_reader.codelists import parse_codelists
from oda_reader.codelists._fetch import _ALL_STATUSES, _fetch_codelist_bytes
from oda_reader.codelists._types import DEFAULT_URL
from oda_reader.common import ImporterPaths
from oda_reader.exceptions import _redact_hidden_token_values
from scripts.data_maintenance._common import dumps_canonical, emit_json_diff

_OECD_URL = DEFAULT_URL

# parse_area_codelist below hands one codelist's bytes to parse_codelists at a
# time, which needs a codelist_id key and a fetched_at timestamp neither of
# which affect the {code: dotstat} projection (codelist_id isn't part of the
# output; fetched_at is metadata only). Fixed placeholders keep the function
# pure -- same input bytes, same output, no wall-clock dependency.
_PLACEHOLDER_CODELIST_ID = "0"
_PLACEHOLDER_FETCHED_AT = datetime.datetime(1970, 1, 1, tzinfo=datetime.UTC)


def _first_present(*values: object) -> str | None:
    """The first value that is neither ``pd.NA`` nor an empty string, or None."""
    for value in values:
        if pd.isna(value):
            continue
        text = str(value)
        if text:
            return text
    return None


# Both dac1 and dac2 use providers(5) union recipients(13) — the committed dac1
# file contains recipient countries (AFG, ALB, AGO...), so dac1 is not
# providers-only.  Providers are listed first so they win on key collision
# within the live union.
_AREA_CODELIST_IDS: dict[str, list[str]] = {
    "dac1": ["5", "13"],
    "dac2": ["5", "13"],
}

_PROVENANCE_PATH = Path(__file__).resolve().parent.parent.parent / (
    "src/oda_reader/schemas/mappings/_provenance.json"
)

# The Cblstatus$N boxes _fetch_codelist_bytes sends by default: all four
# (active, future, heading, withdrawn). Recorded in provenance so a change
# to the request is visible in the sidecar without the field ever changing
# on its own during ordinary runs.
_ASPX_STATUSES_REQUESTED: tuple[str, ...] = ("active", "future", "heading", "withdrawn")


@dataclass(frozen=True, slots=True, kw_only=True)
class RefreshSettings:
    """Runtime settings for the refresh tool."""

    target: str | None = None  # "dac1" | "dac2" | None (both)
    capture_fixtures: Path | None = None
    write: bool = False
    check: bool = False  # exit non-zero if the live data differs from committed
    audit: bool = False  # print the §6 withdrawn-record findings; writes nothing


def fetch_codelist_json(
    codelist_id: str, *, standard: str = "0", timeout: int = 30
) -> bytes:
    """Delegate to the packaged handshake, keeping the script's byte-level seam.

    Existing tests monkeypatch this exact module-level name to inject raw
    bytes. Redirecting through a different entry point would require every
    mock to return a different type, breaking the behaviour-preservation
    gate. This wrapper is the fixed seam that must remain stable.
    """
    return _fetch_codelist_bytes(
        codelist_id=codelist_id, standard=standard, timeout=timeout
    )


def parse_area_codelist(raw_json: bytes) -> dict[str, str]:
    """Parse raw OECD JSON into {str(code): dotstatcode or iso-alpha-3-code}.

    The mapping value is the code the .stat schema uses (the conversion target of
    ``convert_*_to_dotstat_codes``), so ``dotstatcode`` is preferred. It equals the
    ISO3 code for ordinary countries but diverges where OECD's .stat code differs
    from the ISO code (e.g. Kosovo: dotstatcode ``XKV`` vs iso3 ``XKX`` — the .stat
    value is ``XKV``). ``iso-alpha-3-code`` is the fallback, and for multilateral
    organisations without an ISO3 the dotstatcode is the only code (e.g. provider
    807 → ``1UN016``). Only active rows with at least one code field are included.

    Projects the frame built by ``parse_codelists`` rather than walking the
    JSON envelope itself.

    Args:
        raw_json: Raw bytes from fetch_codelist_json.

    Returns:
        Mapping of DAC numeric code (string) to .stat code (string).
    """
    snapshot = parse_codelists(
        raw={_PLACEHOLDER_CODELIST_ID: raw_json},
        fetched_at=_PLACEHOLDER_FETCHED_AT,
    )
    active = snapshot.frame[snapshot.frame["status"] == "active"]
    result: dict[str, str] = {}
    for code, dotstat_code, iso3 in zip(
        active["code"], active["dotstat_code"], active["iso3"], strict=True
    ):
        code_val = _first_present(dotstat_code, iso3)
        if code_val is None:
            continue
        result[str(code)] = code_val
    return result


def build_live_union(target: str, raw_by_id: dict[str, bytes]) -> dict[str, str]:
    """Merge the live area codelists for *target* into one digit-keyed map.

    Providers (codelist 5) are listed first in ``_AREA_CODELIST_IDS`` and win on
    collision within the live union.

    Args:
        target: "dac1" or "dac2".
        raw_by_id: Mapping of codelist_id → raw JSON bytes.

    Returns:
        Mapping of DAC numeric code (string) to .stat code (string).
    """
    live: dict[str, str] = {}
    for cid in _AREA_CODELIST_IDS[target]:
        for k, v in parse_area_codelist(raw_by_id[cid]).items():
            live.setdefault(k, v)
    return live


def build_area_map(
    *,
    target: str,
    raw_by_id: dict[str, bytes],
    committed: dict[str, str],
) -> dict[str, str]:
    """Build the canonical area map: committed codes overlaid with the live union.

    The proposed map = committed union live, so that historical codes absent from
    the current OECD endpoint (Anguilla, Aruba, Bermuda, Cayman Islands, etc.) are
    NEVER dropped -- they are still needed to translate historical ODA data. Live
    values win on conflict (e.g. code 57 XKV->XKX, 918 4EU003->4EU001), surfacing
    genuine updates transparently in the diff.

    Args:
        target: "dac1" or "dac2".
        raw_by_id: Mapping of codelist_id → raw JSON bytes.
        committed: The currently-committed digit-keyed area map (without passthrough
            sentinel); loaded from ``ImporterPaths.mappings / "<target>_codes_area.json"``
            and passed in by ``run()`` to keep this function pure.

    Returns:
        Digit-keyed {str_code: str_dotstat} dict. ``dumps_canonical`` handles the
        int-sort and appends the AREA_PASSTHROUGH sentinel last when rendering it.
    """
    if target not in _AREA_CODELIST_IDS:
        raise ValueError(
            f"unknown target {target!r}; expected one of {set(_AREA_CODELIST_IDS)}"
        )

    committed_digits = {k: v for k, v in committed.items() if k.isdigit()}

    # Start from the full committed digit keys (preserves every historical
    # code) and overlay the live union: live values win on conflict.
    merged: dict[str, str] = dict(committed_digits)
    merged.update(build_live_union(target, raw_by_id))
    return merged


def _render_provenance(
    *,
    last_changed_date: str,
    codelist_ids: list[str],
    statuses_requested: tuple[str, ...] = _ASPX_STATUSES_REQUESTED,
) -> str:
    """Render the provenance sidecar as a JSON string."""
    doc = {
        "source_url": _OECD_URL,
        "aspx_codelist_ids": codelist_ids,
        # When this refresh observed drift from the live OECD source.
        # Not an OECD-supplied timestamp; do not repurpose with one.
        "codelist_last_changed_date": last_changed_date,
        # The Cblstatus$N boxes actually requested. Changes only when the
        # request itself changes, introducing no churn. Deliberately NOT a
        # codelist_last_verified_date, which would dirty every weekly run
        # for no reason (commit 85325d1).
        "aspx_statuses_requested": list(statuses_requested),
    }
    return json.dumps(doc, indent=2, ensure_ascii=True) + "\n"


def _latest_period_per_code(frame: pd.DataFrame, codelist_id: str) -> pd.DataFrame:
    """One row per code in *codelist_id*: its most recent validity period.

    The row grain is ``(codelist_id, code, activation_date)`` — codelist 13
    publishes validity periods, so a code like 130 (Algeria) carries three
    rows: withdrawn 1996-2010, withdrawn 2011-2021, active from 2022.
    "Most recent" means the greatest ``activation_date`` within the code's
    group; a code with only one period (all of codelist 5, and codelist 13's
    code 999) has a ``pd.NA`` ``activation_date`` and no other row to compare
    it against, so that lone row is trivially its own latest period.

    Sorting with ``na_position="first"`` puts a ``pd.NA`` row before any
    real date within its group, so ``tail(1)`` always keeps the greatest
    real date when one exists, and falls back to the sole ``pd.NA`` row
    when it doesn't.
    """
    subset = frame[frame["codelist_id"] == codelist_id]
    return (
        subset.sort_values("activation_date", na_position="first")
        .groupby("code")
        .tail(1)
    )


def _run_audit(*, raw_by_id: dict[str, bytes], targets: list[str]) -> int:
    """Print three audit findings for *targets* against ``raw_by_id``. Writes no file.

    Reuses the payload ``run()`` already fetched (the all-status request),
    so auditing costs nothing beyond the parse already needed for the
    ordinary diff.

    A code counts as **retired** when its latest period (see
    ``_latest_period_per_code``) is withdrawn — not when *any* period is.
    A reactivated code such as Algeria (withdrawn twice, active since 2022)
    must not be flagged as a withdrawal candidate just because history
    contains a withdrawn period; only its current state matters. Findings
    1 and 3 both run over this retired set.

    1. **Conflict check.** For every retired code with a usable value
       (``dotstat_code`` or ``iso3``) that is also in the committed map,
       assert the committed value equals the retired code's value. A
       non-empty mismatch set means OECD restated a historical code's
       value and our translation of historical data is now wrong.
    2. **Orphan count.** Committed codes absent from *every* row of the
       fetched payload — active, withdrawn, future or heading. Should grow
       slowly; a jump means OECD pruned their own list.
    3. **New-withdrawn candidates.** Retired codes with a usable value not
       present in the committed map. These need a human: adding a
       withdrawn code to a translation map is a judgement about whether
       historical data uses it.

    Returns:
        1 if any committed value conflicts with a retired code's live
        value — the one finding that signals an actual correctness
        problem. Orphans and new-withdrawn candidates are reported but
        never fail the run; both require a human judgement call, not an
        automatic reaction.
    """
    snapshot = parse_codelists(
        raw=raw_by_id,
        fetched_at=datetime.datetime.now(datetime.UTC),
        source_url=_OECD_URL,
    )
    frame = snapshot.frame

    any_conflicts = False
    for target in targets:
        area_path = ImporterPaths.mappings / f"{target}_codes_area.json"
        committed_full: dict[str, str] = (
            json.loads(area_path.read_text(encoding="utf-8"))
            if area_path.exists()
            else {}
        )
        committed_digits = {k: v for k, v in committed_full.items() if k.isdigit()}

        codes_seen: set[str] = set()
        retired_parts: list[pd.DataFrame] = []
        for cid in _AREA_CODELIST_IDS[target]:
            subset = frame[frame["codelist_id"] == cid]
            codes_seen.update(str(code) for code in subset["code"])
            latest = _latest_period_per_code(frame, cid)
            retired_parts.append(latest[latest["status"] == "withdrawn"])
        retired = pd.concat(retired_parts)

        comparisons = 0
        conflicts: list[tuple[str, str, str]] = []
        new_candidates: dict[str, str] = {}
        for _, row in retired.iterrows():
            value = _first_present(row["dotstat_code"], row["iso3"])
            if value is None:
                continue
            code = str(row["code"])
            if code in committed_digits:
                comparisons += 1
                if committed_digits[code] != value:
                    conflicts.append((code, committed_digits[code], value))
            else:
                new_candidates[code] = value

        orphans = sorted((k for k in committed_digits if k not in codes_seen), key=int)

        print(
            f"[{target}] audit: {len(retired)} retired codes"
            f" ({comparisons} committed comparisons, {len(conflicts)} conflicts,"
            f" {len(orphans)} orphans, {len(new_candidates)} new-withdrawn candidates)",
            file=sys.stderr,
        )
        for code, committed_value, live_value in conflicts:
            print(
                f"  CONFLICT code {code}: committed {committed_value!r} !="
                f" withdrawn {live_value!r}",
                file=sys.stderr,
            )
        if orphans:
            print(f"  orphans: {', '.join(orphans)}", file=sys.stderr)
        for code, value in sorted(
            new_candidates.items(), key=lambda item: int(item[0])
        ):
            print(f"  new-withdrawn candidate: {code} -> {value}", file=sys.stderr)

        if conflicts:
            any_conflicts = True

    return 1 if any_conflicts else 0


def _validate_settings(settings: RefreshSettings) -> str | None:
    """The error message for an invalid *settings*, or None if it's fine."""
    if settings.target is not None and settings.target not in _AREA_CODELIST_IDS:
        return (
            f"error: unknown --target {settings.target!r}; "
            f"choose from {sorted(_AREA_CODELIST_IDS)}"
        )
    if settings.audit and settings.write:
        return (
            "error: --audit and --write are mutually exclusive"
            " (--audit changes no file)"
        )
    return None


def run(*, settings: RefreshSettings) -> int:
    """Fetch OECD DAC area codelists and print diffs against committed mappings.

    Returns:
        0 on success, 1 on error.
    """
    error = _validate_settings(settings)
    if error is not None:
        print(error, file=sys.stderr)
        return 1

    targets = (
        list(_AREA_CODELIST_IDS.keys())
        if settings.target is None
        else [settings.target]
    )

    # Collect the set of codelist IDs we actually need
    needed_ids: list[str] = []
    for t in targets:
        for cid in _AREA_CODELIST_IDS[t]:
            if cid not in needed_ids:
                needed_ids.append(cid)

    capture_dir = settings.capture_fixtures
    if capture_dir is not None:
        capture_dir.mkdir(parents=True, exist_ok=True)

    raw_by_id: dict[str, bytes] = {}
    html_captured = False

    for cid in needed_ids:
        print(f"Fetching codelist {cid}...", file=sys.stderr)
        capture: dict[str, str] = {}
        try:
            if capture_dir is not None:
                # Fixture capture requests all four Cblstatus$N boxes
                # (status is merge content). Calls _fetch_codelist_bytes
                # directly rather than going through the frozen
                # fetch_codelist_json seam, whose signature does not carry
                # the private _statuses parameter. The HTML capture hook is
                # only wired up for the first codelist — page_step{1,2}.html
                # are one file each, not one per codelist.
                raw = _fetch_codelist_bytes(
                    cid,
                    _capture=None if html_captured else capture,
                    _statuses=_ALL_STATUSES,
                )
                html_captured = True
            else:
                raw = fetch_codelist_json(cid)
        except Exception as exc:
            print(f"error fetching codelist {cid}: {exc}", file=sys.stderr)
            return 1

        if capture_dir is not None:
            out_path = capture_dir / f"codelist_{cid}.json"
            out_path.write_bytes(raw)
            print(f"  wrote {out_path}", file=sys.stderr)
            for name, html in capture.items():
                html_path = capture_dir / f"{name}.html"
                # Every hidden input whose name starts with "__" gets its
                # value replaced, so no live __VIEWSTATE reaches the
                # committed fixture. Redaction reuses the same helper the
                # runtime exception path applies to `body`
                # (src/oda_reader/exceptions.py).
                redacted = _redact_hidden_token_values(html.encode("utf-8"))
                html_path.write_text(redacted.decode("utf-8"), encoding="utf-8")
                print(f"  wrote {html_path}", file=sys.stderr)
        else:
            raw_by_id[cid] = raw

    # --capture-fixtures already wrote its files inside the loop above, and
    # --audit computes and prints its own findings from the same
    # raw_by_id -- neither continues into the diff/write path below.
    if capture_dir is not None:
        early_exit_code = 0
    elif settings.audit:
        early_exit_code = _run_audit(raw_by_id=raw_by_id, targets=targets)
    else:
        early_exit_code = None
    if early_exit_code is not None:
        return early_exit_code

    drift_detected = False
    # A shape change in the live payload now surfaces as a diagnosable
    # message and exit code 1 in this unattended weekly job, rather than a
    # bare traceback several frames from the cause.
    try:
        for target in targets:
            area_path = ImporterPaths.mappings / f"{target}_codes_area.json"
            original_text = (
                area_path.read_text(encoding="utf-8") if area_path.exists() else ""
            )
            # Load the committed digit-keyed map; drop passthrough —
            # build_area_map (via dumps_canonical) re-appends it as the last
            # entry.
            committed_full: dict[str, str] = (
                json.loads(original_text) if original_text else {}
            )
            committed_digits = {k: v for k, v in committed_full.items() if k.isdigit()}

            # Count how many live codes this target fetches
            live_union = build_live_union(target, raw_by_id)

            proposed_map = build_area_map(
                target=target, raw_by_id=raw_by_id, committed=committed_digits
            )
            proposed_text = dumps_canonical(proposed_map)

            # Per-target summary
            added = [k for k in live_union if k not in committed_digits]
            changed = [
                k
                for k in live_union
                if k in committed_digits and live_union[k] != committed_digits[k]
            ]
            historical = [k for k in committed_digits if k not in live_union]
            print(
                f"[{target}] {len(live_union)} live, {len(committed_digits)} committed"
                f" → +{len(added)} added, {len(changed)} changed,"
                f" {len(historical)} historical preserved",
                file=sys.stderr,
            )
            if changed:
                for k in changed:
                    print(
                        f"  changed {k}: {committed_digits[k]!r} → {live_union[k]!r}",
                        file=sys.stderr,
                    )

            if original_text == proposed_text:
                print(f"[{target}] No changes detected.", file=sys.stderr)
            else:
                drift_detected = True
                emit_json_diff(
                    original_text,
                    proposed_text,
                    fromfile=f"{target}_codes_area.json (current)",
                    tofile=f"{target}_codes_area.json (proposed)",
                )
                if settings.write:
                    area_path.write_text(proposed_text, encoding="utf-8")
                    print(f"[{target}] wrote {area_path}", file=sys.stderr)
    except Exception as exc:
        print(f"error building area map for {target}: {exc}", file=sys.stderr)
        return 1

    if settings.write and drift_detected:
        # Update provenance sidecar with timestamp and fetched IDs.
        all_ids = sorted(
            {cid for t in targets for cid in _AREA_CODELIST_IDS[t]}, key=int
        )
        last_changed_date = datetime.date.today().isoformat()
        prov_text = _render_provenance(
            last_changed_date=last_changed_date,
            codelist_ids=all_ids,
        )
        _PROVENANCE_PATH.write_text(prov_text, encoding="utf-8")
        print(f"wrote {_PROVENANCE_PATH}", file=sys.stderr)

    if settings.check and drift_detected:
        print(
            "error: committed codelists are out of date with the live OECD source; "
            "run `python -m scripts.data_maintenance.refresh_dac_codelists --write` "
            "and review the diff.",
            file=sys.stderr,
        )
        return 1

    return 0


def main(argv: list[str] | None = None) -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Refresh OECD DAC area codelist mappings."
    )
    parser.add_argument(
        "--target",
        choices=list(_AREA_CODELIST_IDS.keys()),
        default=None,
        help="Which target to refresh (default: both dac1 and dac2).",
    )
    parser.add_argument(
        "--capture-fixtures",
        metavar="DIR",
        type=Path,
        default=None,
        help="Write raw codelist JSON to DIR and exit (for offline testing).",
    )
    parser.add_argument(
        "--write",
        action="store_true",
        default=False,
        help="Write proposed changes to the mappings directory (default: diff only).",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        default=False,
        help="Exit non-zero if the live source differs from committed (for CI drift checks).",
    )
    parser.add_argument(
        "--audit",
        action="store_true",
        default=False,
        help=(
            "Print the withdrawn-record conflict/orphan/new-candidate findings"
            " and exit. Writes no file; mutually exclusive with --write."
        ),
    )
    args = parser.parse_args(argv)
    settings = RefreshSettings(
        target=args.target,
        capture_fixtures=args.capture_fixtures,
        write=args.write,
        check=args.check,
        audit=args.audit,
    )
    sys.exit(run(settings=settings))


if __name__ == "__main__":
    main()
