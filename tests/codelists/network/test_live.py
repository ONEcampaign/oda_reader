"""Live tests against the real OECD codelist app.

`@pytest.mark.network`, opt-in via `RUN_NETWORK_TESTS=1` — the repo's existing pattern for
gating network-touching tests. Skip by default; set `RUN_NETWORK_TESTS=1` to run.
"""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path

import pytest

from oda_reader.codelists import (
    fetch_codelists,
    fetch_provider_agencies,
    parse_codelists,
    parse_provider_agencies,
)

_FIXTURES = Path(__file__).parent.parent.parent / "fixtures" / "oecd"
_FETCHED_AT = datetime(2026, 1, 1, tzinfo=UTC)
_KNOWN_STATUSES = {"active", "withdrawn", "future", "heading"}

pytestmark = pytest.mark.network


def _skip_if_no_network() -> None:
    if os.environ.get("RUN_NETWORK_TESTS") != "1":
        pytest.skip("set RUN_NETWORK_TESTS=1 to run network tests")


def _committed_fixture_snapshot():
    """The committed codelist_5/13.json fixtures, parsed for comparison against a live fetch."""
    for filename in ("codelist_5.json", "codelist_13.json"):
        if not (_FIXTURES / filename).exists():
            pytest.skip(
                f"fixture {filename} not yet captured — run --capture-fixtures first"
            )
    raw = {
        "5": (_FIXTURES / "codelist_5.json").read_bytes(),
        "13": (_FIXTURES / "codelist_13.json").read_bytes(),
    }
    return parse_codelists(raw=raw, fetched_at=_FETCHED_AT)


@pytest.mark.network
def test_fetch_codelists_against_the_real_oecd_app() -> None:
    """Live fetch returns both codelists with row counts above a floor, every `status`
    value in the known domain, `unknown_statuses` empty, plus a field-drift canary. The
    live key set must be a superset of the fixture's, catching field disappearance upstream
    before it becomes an unnoticed null column."""
    _skip_if_no_network()

    snapshot = fetch_codelists()
    assert snapshot.codelist_ids == ("5", "13")

    counts = snapshot.frame["codelist_id"].value_counts()
    # A floor, not an exact match — OECD's live data moves. This validates the fixture
    # is still representative, without imposing strict row-count requirements on library code.
    assert counts.get("5", 0) > 100, "codelist 5 returned suspiciously few rows"
    assert counts.get("13", 0) > 100, "codelist 13 returned suspiciously few rows"

    observed_statuses = set(snapshot.frame["status"].unique())
    assert observed_statuses <= _KNOWN_STATUSES, (
        f"unexpected status value(s) outside {_KNOWN_STATUSES}: "
        f"{observed_statuses - _KNOWN_STATUSES}"
    )
    assert snapshot.unknown_statuses == ()

    fixture = _committed_fixture_snapshot()
    live_keys = set(
        zip(
            snapshot.frame["codelist_id"],
            snapshot.frame["code"],
            snapshot.frame["activation_date"].astype(str),
            strict=True,
        )
    )
    fixture_keys = set(
        zip(
            fixture.frame["codelist_id"],
            fixture.frame["code"],
            fixture.frame["activation_date"].astype(str),
            strict=True,
        )
    )
    missing = fixture_keys - live_keys
    assert not missing, (
        f"{len(missing)} key(s) present in the committed fixture but absent from the live "
        f"payload — possible field drift upstream: {sorted(missing)[:10]}"
    )


def _reconstructed_agency_snapshot():
    """Codelist 16 (Provider agency), reconstructed from block index 1 of the
    committed `codelist_0.json` "All codes list" fixture -- the same source
    `test_agencies.py` builds its offline coverage from. No dedicated
    `codelist_16.json` fixture exists: nobody has ever issued a standalone
    single-16 request against the live app, so this live test is what
    actually proves the reconstruction matches what the server returns for
    one -- the reconstructed key set being checked here isn't just convenient
    reuse, it's the only evidence either payload shape is trustworthy."""
    path = _FIXTURES / "codelist_0.json"
    if not path.exists():
        pytest.skip(
            "fixture codelist_0.json not yet captured — run --capture-fixtures first"
        )
    payload = json.loads(path.read_bytes())
    block = payload["codelists"]["codelist"][1]
    assert block["name"] == "Provider agency"
    envelope = json.dumps({"codelists": {"codelist": [block]}}).encode()
    return parse_provider_agencies(raw={"16": envelope}, fetched_at=_FETCHED_AT)


@pytest.mark.network
def test_fetch_provider_agencies_against_the_real_oecd_app() -> None:
    """Live fetch of codelist 16 (Provider agency): a row-count floor, every
    `status` value in the known domain, and the live key set a superset of
    the reconstructed one -- the same field-drift canary
    `test_fetch_codelists_against_the_real_oecd_app` runs for the area
    contract, adapted to the agency contract's four-column key."""
    _skip_if_no_network()

    snapshot = fetch_provider_agencies()
    assert snapshot.codelist_ids == ("16",)

    # A floor, not an exact match — OECD's live data moves. 1,374 is the
    # reconstructed fixture's row count; this validates it is still
    # representative, without imposing strict row-count requirements.
    assert len(snapshot.frame) > 1000, "codelist 16 returned suspiciously few rows"

    observed_statuses = set(snapshot.frame["status"].unique())
    assert observed_statuses <= _KNOWN_STATUSES, (
        f"unexpected status value(s) outside {_KNOWN_STATUSES}: "
        f"{observed_statuses - _KNOWN_STATUSES}"
    )

    reconstructed = _reconstructed_agency_snapshot()
    live_keys = set(
        zip(
            snapshot.frame["donor_code"],
            snapshot.frame["code"],
            snapshot.frame["activation_date"].astype(str),
            strict=True,
        )
    )
    reconstructed_keys = set(
        zip(
            reconstructed.frame["donor_code"],
            reconstructed.frame["code"],
            reconstructed.frame["activation_date"].astype(str),
            strict=True,
        )
    )
    missing = reconstructed_keys - live_keys
    assert not missing, (
        f"{len(missing)} key(s) present in the reconstructed fixture but absent from the "
        f"live payload — possible field drift upstream: {sorted(missing)[:10]}"
    )
