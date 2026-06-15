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
"""

from __future__ import annotations

import argparse
import datetime
import http.cookiejar
import json
import re
import sys
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from oda_reader.common import ImporterPaths
from scripts.data_maintenance._common import dumps_canonical, emit_json_diff

_OECD_URL = "https://development-finance-codelists.oecd.org/CodesList.aspx"

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


@dataclass(frozen=True, slots=True, kw_only=True)
class RefreshSettings:
    """Runtime settings for the refresh tool."""

    target: str | None = None  # "dac1" | "dac2" | None (both)
    capture_fixtures: Path | None = None
    write: bool = False
    check: bool = False  # exit non-zero if the live data differs from committed


def fetch_codelist_json(
    codelist_id: str, *, standard: str = "0", timeout: int = 30
) -> bytes:
    """Fetch a codelist from the OECD codelist app via the ASPX two-step handshake.

    Args:
        codelist_id: OECD codelist ID (e.g. "5" for providers, "13" for recipients).
        standard: DDL_CRSTOSSD value (default "0").
        timeout: Per-request timeout in seconds.

    Returns:
        Raw JSON bytes from the OECD server.
    """
    cj = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
    opener.addheaders = [("User-Agent", "oda-importer/refresh-dac-codelists")]

    def _hidden(html: str, name: str) -> str:
        m = re.search(rf'<input[^>]*name="{re.escape(name)}"[^>]*value="([^"]*)"', html)
        return m.group(1) if m else ""

    # Step 1: GET the page to collect initial VIEWSTATE tokens
    with opener.open(_OECD_URL, timeout=timeout) as r:
        html = r.read().decode("utf-8", "replace")

    base: dict[str, str] = {
        "__EVENTARGUMENT": "",
        "__LASTFOCUS": "",
        "__VIEWSTATE": _hidden(html, "__VIEWSTATE"),
        "__VIEWSTATEGENERATOR": _hidden(html, "__VIEWSTATEGENERATOR"),
        "__VIEWSTATEENCRYPTED": "",
        "__EVENTVALIDATION": _hidden(html, "__EVENTVALIDATION"),
        "DDl_codeslist": codelist_id,
        "DDL_CRSTOSSD": standard,
        "Cblstatus$0": "on",
        "Cblstatus$2": "on",
        "tb_search": "",
    }

    # Step 2: POST to select the codelist (triggers VIEWSTATE re-seed)
    p1 = dict(base, __EVENTTARGET="DDl_codeslist")
    with opener.open(
        urllib.request.Request(
            _OECD_URL,
            data=urllib.parse.urlencode(p1).encode(),
            method="POST",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        ),
        timeout=timeout,
    ) as r2:
        h2 = r2.read().decode("utf-8", "replace")

    # Re-scrape tokens from the updated page
    base["__VIEWSTATE"] = _hidden(h2, "__VIEWSTATE")
    base["__VIEWSTATEGENERATOR"] = _hidden(h2, "__VIEWSTATEGENERATOR")
    base["__EVENTVALIDATION"] = _hidden(h2, "__EVENTVALIDATION")

    # Step 3: POST to request JSON download
    p2 = dict(base, __EVENTTARGET="", b_json="JSON")
    with opener.open(
        urllib.request.Request(
            _OECD_URL,
            data=urllib.parse.urlencode(p2).encode(),
            method="POST",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        ),
        timeout=timeout,
    ) as r3:
        return r3.read()


def _unwrap_rows(raw_json: bytes) -> tuple[list[dict[str, Any]], str | None]:
    """Unwrap the OECD JSON envelope into (rows, date_last_modified)."""
    payload = json.loads(raw_json)
    codelist_list = payload["codelists"]["codelist"]
    if not codelist_list:
        raise ValueError(
            "Expected at least one codelist in payload['codelists']['codelist']; got empty list"
        )
    codelist_node = codelist_list[0]
    rows: list[dict[str, Any]] = codelist_node["codelist-items"]["codelist-item"]
    date_last_modified: str | None = payload["codelists"].get("date-last-modified")
    return rows, date_last_modified


def parse_area_codelist(raw_json: bytes) -> dict[str, str]:
    """Parse raw OECD JSON into {str(code): dotstatcode or iso-alpha-3-code}.

    The mapping value is the code the .stat schema uses (the conversion target of
    ``convert_*_to_dotstat_codes``), so ``dotstatcode`` is preferred. It equals the
    ISO3 code for ordinary countries but diverges where OECD's .stat code differs
    from the ISO code (e.g. Kosovo: dotstatcode ``XKV`` vs iso3 ``XKX`` — the .stat
    value is ``XKV``). ``iso-alpha-3-code`` is the fallback, and for multilateral
    organisations without an ISO3 the dotstatcode is the only code (e.g. provider
    807 → ``1UN016``). Only active rows with at least one code field are included.

    Args:
        raw_json: Raw bytes from fetch_codelist_json.

    Returns:
        Mapping of DAC numeric code (string) to .stat code (string).
    """
    rows, _ = _unwrap_rows(raw_json)
    result: dict[str, str] = {}
    for row in rows:
        if row.get("status", "").lower() != "active":
            continue
        dotstat: str | None = row.get("dotstatcode") or None
        iso3: str | None = row.get("iso-alpha-3-code") or None
        code_val = dotstat or iso3
        if code_val is None:
            continue
        result[str(row["code"])] = code_val
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
    """Build the canonical area map using additive-merge semantics.

    The proposed map = committed union live, so that historical codes absent from
    the current OECD endpoint (Anguilla, Aruba, Bermuda, Cayman Islands, etc.)
    are NEVER dropped -- they are still needed to translate historical ODA data.
    Live values win on conflict (e.g. code 57 XKV->XKX, 918 4EU003->4EU001),
    surfacing genuine updates transparently in the diff.

    Merge order:
    1. Start from committed digit-keyed entries (preserves every historical code).
    2. Build the live union (providers first; providers win on collision within live).
    3. Overlay live onto committed (``merged.update(live)``) so live wins on conflict.
    4. ``dumps_canonical`` handles int-sort and appends AREA_PASSTHROUGH last.

    Args:
        target: "dac1" or "dac2".
        raw_by_id: Mapping of codelist_id → raw JSON bytes.
        committed: The currently-committed digit-keyed area map (without passthrough
            sentinel); loaded from ``ImporterPaths.mappings / "<target>_codes_area.json"``
            and passed in by ``run()`` to keep this function pure.

    Returns:
        Ordered {str_code: str_dotstat} dict, digit keys ascending then ``(.*)`` last.
    """
    if target not in _AREA_CODELIST_IDS:
        raise ValueError(
            f"unknown target {target!r}; expected one of {set(_AREA_CODELIST_IDS)}"
        )

    live = build_live_union(target, raw_by_id)

    # Start from committed digit keys (drop passthrough sentinel if present —
    # dumps_canonical re-appends it).
    merged: dict[str, str] = {k: v for k, v in committed.items() if k.isdigit()}
    # Overlay live: live values win on conflict.
    merged.update(live)
    return merged


def _render_provenance(
    *,
    query_date: str,
    codelist_ids: list[str],
    date_last_modified: str | None,
) -> str:
    """Render the provenance sidecar as a JSON string."""
    doc = {
        "source_url": _OECD_URL,
        "aspx_codelist_ids": codelist_ids,
        "oecd_query_date": query_date,
        "codelist_date_last_modified": date_last_modified,
    }
    return json.dumps(doc, indent=2, ensure_ascii=True) + "\n"


def run(*, settings: RefreshSettings) -> int:
    """Fetch OECD DAC area codelists and print diffs against committed mappings.

    Returns:
        0 on success, 1 on error.
    """
    targets = (
        list(_AREA_CODELIST_IDS.keys())
        if settings.target is None
        else [settings.target]
    )
    if settings.target is not None and settings.target not in _AREA_CODELIST_IDS:
        print(
            f"error: unknown --target {settings.target!r}; "
            f"choose from {sorted(_AREA_CODELIST_IDS)}",
            file=sys.stderr,
        )
        return 1

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
    last_modified_by_id: dict[str, str | None] = {}

    for cid in needed_ids:
        print(f"Fetching codelist {cid}...", file=sys.stderr)
        try:
            raw = fetch_codelist_json(cid)
        except Exception as exc:
            print(f"error fetching codelist {cid}: {exc}", file=sys.stderr)
            return 1

        if capture_dir is not None:
            out_path = capture_dir / f"codelist_{cid}.json"
            out_path.write_bytes(raw)
            print(f"  wrote {out_path}", file=sys.stderr)
        else:
            raw_by_id[cid] = raw
            _, dlm = _unwrap_rows(raw)
            last_modified_by_id[cid] = dlm

    if capture_dir is not None:
        return 0

    query_date = datetime.date.today().isoformat()

    drift_detected = False
    for target in targets:
        area_path = ImporterPaths.mappings / f"{target}_codes_area.json"
        original_text = (
            area_path.read_text(encoding="utf-8") if area_path.exists() else ""
        )
        # Load the committed digit-keyed map; drop passthrough — build_area_map
        # (via dumps_canonical) re-appends it as the last entry.
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

    if settings.write:
        # Update provenance sidecar with all fetched IDs
        all_ids = sorted(
            {cid for t in targets for cid in _AREA_CODELIST_IDS[t]}, key=int
        )
        dlm = next((v for v in last_modified_by_id.values() if v is not None), None)
        prov_text = _render_provenance(
            query_date=query_date,
            codelist_ids=all_ids,
            date_last_modified=dlm,
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
    args = parser.parse_args(argv)
    settings = RefreshSettings(
        target=args.target,
        capture_fixtures=args.capture_fixtures,
        write=args.write,
        check=args.check,
    )
    sys.exit(run(settings=settings))


if __name__ == "__main__":
    main()
