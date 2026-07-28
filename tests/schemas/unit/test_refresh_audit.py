"""Offline tests for `--audit` mode.

All tests are @pytest.mark.unit and require no network access. The `--audit` mode reads
whatever codelists were already fetched, so tests supply payloads via a monkeypatched
`fetch_codelist_json`, matching the pattern in `test_refresh_dac_codelists.py`. The
fixture-backed test skips cleanly when the committed fixtures are absent.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

import scripts.data_maintenance.refresh_dac_codelists as mod
from scripts.data_maintenance.refresh_dac_codelists import RefreshSettings, run

_FIXTURES = Path(__file__).parent.parent.parent / "fixtures" / "oecd"
_MAPPINGS = (
    Path(__file__).parent.parent.parent.parent
    / "src"
    / "oda_reader"
    / "schemas"
    / "mappings"
)


def _load_fixture(filename: str) -> bytes:
    path = _FIXTURES / filename
    if not path.exists():
        pytest.skip(
            f"Fixture {filename} not yet captured — run --capture-fixtures first"
        )
    return path.read_bytes()


# ---------------------------------------------------------------------------
# Inline envelope helpers (no fixtures needed)
# ---------------------------------------------------------------------------


def _envelope(name: str, *items: dict) -> bytes:
    """Wrap codelist-item dicts in the OECD JSON envelope shape."""
    return json.dumps(
        {
            "codelists": {
                "date-last-modified": "2026-01-01",
                "codelist": [
                    {"name": name, "codelist-items": {"codelist-item": list(items)}}
                ],
            }
        }
    ).encode()


def _row(
    code: str, *, status: str, dotstat: str, activation_date: str | None = None
) -> dict:
    row = {
        "status": status,
        "code": code,
        "name": {"narrative": [f"Code {code}"]},
        "type": "Recipient",
        "iso-alpha-3-code": None,
        "dotstatcode": dotstat,
        "crs": "1",
        "tossd": "1",
    }
    if activation_date is not None:
        row["activation-date"] = activation_date
    return row


# codelist 5 only needs to be non-empty (a zero-item codelist is a
# CodelistValidationError, not a valid "nothing here" payload) and must not
# interact with the codelist-13 scenario below.
_CODELIST_5 = _envelope("Providers", _row("999", status="Active", dotstat="ZZZ"))


def _mock_fetch(codelist_13_items: bytes):
    """A `fetch_codelist_json` stand-in serving `_CODELIST_5` and *codelist_13_items*."""

    def fake_fetch(
        codelist_id: str, *, standard: str = "0", timeout: int = 30
    ) -> bytes:
        return {"5": _CODELIST_5, "13": codelist_13_items}[codelist_id]

    return fake_fetch


def _write_committed(tmp_path: Path, target: str, committed: dict[str, str]) -> Path:
    fake_mappings = tmp_path / "mappings"
    fake_mappings.mkdir(exist_ok=True)
    path = fake_mappings / f"{target}_codes_area.json"
    path.write_text(json.dumps(committed), encoding="utf-8")
    return fake_mappings


# ---------------------------------------------------------------------------
# The three §6 findings
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_audit_reports_conflict_when_committed_value_disagrees(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    """A retired code whose committed value differs from the live one is a conflict."""
    codelist_13 = _envelope("Recipients", _row("4", status="withdrawn", dotstat="DDD"))
    fake_mappings = _write_committed(tmp_path, "dac1", {"4": "OLD_VALUE"})
    monkeypatch.setattr(mod, "fetch_codelist_json", _mock_fetch(codelist_13))
    monkeypatch.setattr(mod.ImporterPaths, "mappings", fake_mappings)

    rc = run(settings=RefreshSettings(target="dac1", audit=True))

    captured = capsys.readouterr()
    assert "1 conflicts" in captured.err
    assert "CONFLICT code 4: committed 'OLD_VALUE' != withdrawn 'DDD'" in captured.err
    # A value conflict is the only finding that fails the run — it signals that
    # a historical translation has become incorrect.
    assert rc == 1


@pytest.mark.unit
def test_audit_no_conflict_when_committed_value_agrees(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    """A retired code whose committed value matches is a comparison, not a conflict."""
    codelist_13 = _envelope("Recipients", _row("3", status="withdrawn", dotstat="CCC"))
    fake_mappings = _write_committed(tmp_path, "dac1", {"3": "CCC"})
    monkeypatch.setattr(mod, "fetch_codelist_json", _mock_fetch(codelist_13))
    monkeypatch.setattr(mod.ImporterPaths, "mappings", fake_mappings)

    rc = run(settings=RefreshSettings(target="dac1", audit=True))

    captured = capsys.readouterr()
    assert "1 committed comparisons, 0 conflicts" in captured.err
    assert "CONFLICT" not in captured.err
    assert rc == 0


@pytest.mark.unit
def test_audit_reports_orphan_code_absent_from_every_row(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    """A committed code that appears in no row of either codelist is an orphan."""
    codelist_13 = _envelope("Recipients", _row("3", status="withdrawn", dotstat="CCC"))
    # "6" never appears in codelist 5 or 13 at all, active or withdrawn.
    fake_mappings = _write_committed(tmp_path, "dac1", {"3": "CCC", "6": "ORPHANVAL"})
    monkeypatch.setattr(mod, "fetch_codelist_json", _mock_fetch(codelist_13))
    monkeypatch.setattr(mod.ImporterPaths, "mappings", fake_mappings)

    rc = run(settings=RefreshSettings(target="dac1", audit=True))

    captured = capsys.readouterr()
    assert "1 orphans" in captured.err
    assert "orphans: 6" in captured.err
    assert rc == 0


@pytest.mark.unit
def test_audit_reports_new_withdrawn_candidate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    """A retired code absent from the committed map is a new-withdrawn candidate."""
    codelist_13 = _envelope("Recipients", _row("5", status="withdrawn", dotstat="EEE"))
    fake_mappings = _write_committed(tmp_path, "dac1", {})
    monkeypatch.setattr(mod, "fetch_codelist_json", _mock_fetch(codelist_13))
    monkeypatch.setattr(mod.ImporterPaths, "mappings", fake_mappings)

    rc = run(settings=RefreshSettings(target="dac1", audit=True))

    captured = capsys.readouterr()
    assert "1 new-withdrawn candidates" in captured.err
    assert "new-withdrawn candidate: 5 -> EEE" in captured.err
    # A candidate needs a human, not a failed run.
    assert rc == 0


@pytest.mark.unit
def test_audit_reactivated_code_is_not_retired(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    """A code withdrawn once but active in its latest period is not a candidate.

    Regression test for the row-grain logic: code 130 (Algeria) was withdrawn 1996-2010
    and 2011-2021, then active from 2022. If the audit checks whether *any* historical
    period was withdrawn instead of only the latest period, every reactivated code becomes
    a false "new-withdrawn candidate".
    """
    codelist_13 = _envelope(
        "Recipients",
        _row("2", status="withdrawn", dotstat="BBB", activation_date="1996-01-01"),
        _row("2", status="active", dotstat="BBB", activation_date="2022-01-01"),
    )
    fake_mappings = _write_committed(tmp_path, "dac1", {})
    monkeypatch.setattr(mod, "fetch_codelist_json", _mock_fetch(codelist_13))
    monkeypatch.setattr(mod.ImporterPaths, "mappings", fake_mappings)

    rc = run(settings=RefreshSettings(target="dac1", audit=True))

    captured = capsys.readouterr()
    assert "0 retired codes" in captured.err
    assert "0 new-withdrawn candidates" in captured.err
    assert "new-withdrawn candidate:" not in captured.err
    assert rc == 0


# ---------------------------------------------------------------------------
# --audit changes no file; --audit + --write is rejected
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_audit_changes_no_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """--audit must not touch any mapping file, byte-for-byte."""
    codelist_13 = _envelope("Recipients", _row("4", status="withdrawn", dotstat="DDD"))
    fake_mappings = _write_committed(tmp_path, "dac1", {"4": "OLD_VALUE"})
    monkeypatch.setattr(mod, "fetch_codelist_json", _mock_fetch(codelist_13))
    monkeypatch.setattr(mod.ImporterPaths, "mappings", fake_mappings)

    area_path = fake_mappings / "dac1_codes_area.json"
    before_bytes = area_path.read_bytes()
    before_mtime = area_path.stat().st_mtime_ns

    run(settings=RefreshSettings(target="dac1", audit=True))

    assert area_path.read_bytes() == before_bytes
    assert area_path.stat().st_mtime_ns == before_mtime


@pytest.mark.unit
def test_audit_and_write_are_mutually_exclusive(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    """--audit and --write together must error out rather than guessing."""

    def fail_fetch(
        codelist_id: str, *, standard: str = "0", timeout: int = 30
    ) -> bytes:
        raise AssertionError("must not fetch when --audit and --write conflict")

    monkeypatch.setattr(mod, "fetch_codelist_json", fail_fetch)

    rc = run(settings=RefreshSettings(audit=True, write=True))

    assert rc == 1
    captured = capsys.readouterr()
    assert "mutually exclusive" in captured.err.lower()


# ---------------------------------------------------------------------------
# Fixture-backed reproduction of the measured result
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_audit_against_committed_fixtures_reproduces_measured_result(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    """Reproduces the numbers measured against the committed fixtures.

    Codelist 13 carries 364 rows over 207 distinct codes; 30 of those codes are genuinely
    retired (their latest validity period is withdrawn). All 30 carry a usable
    `dotstat_code`/`iso3`; 29 are already committed in `dac2_codes_area.json`; exactly one
    is net-new: code 105 -> MADCT_X. Zero conflicts is the result today — the 29-of-30
    match is the evidence that the never-delete merge still agrees with OECD's withdrawn
    record.

    Note: There are two valid ways to count withdrawn codes. The "187 withdrawn rows"
    counts every row whose status is withdrawn, including superseded periods (e.g. a
    reactivated code's earlier withdrawn period). The "30 retired codes" counts distinct
    codes whose *latest* validity period is withdrawn. Both numbers are correct for their
    purpose; the audit reports the latter since it reflects current policy.
    """
    raw_5 = _load_fixture("codelist_5.json")
    raw_13 = _load_fixture("codelist_13.json")

    def fake_fetch(
        codelist_id: str, *, standard: str = "0", timeout: int = 30
    ) -> bytes:
        return {"5": raw_5, "13": raw_13}[codelist_id]

    monkeypatch.setattr(mod, "fetch_codelist_json", fake_fetch)
    monkeypatch.setattr(mod.ImporterPaths, "mappings", _MAPPINGS)

    rc = run(settings=RefreshSettings(target="dac2", audit=True))

    captured = capsys.readouterr()
    assert "30 retired codes (29 committed comparisons, 0 conflicts" in captured.err, (
        captured.err
    )
    assert "1 new-withdrawn candidates" in captured.err
    assert "new-withdrawn candidate: 105 -> MADCT_X" in captured.err
    assert "CONFLICT" not in captured.err
    assert rc == 0
