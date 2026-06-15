"""Offline parse/build/diff/static-guard tests for refresh_dac_codelists.

All tests are @pytest.mark.unit and require no network access.
Fixture-backed tests skip automatically when the fixture files are absent
(run --capture-fixtures once to populate them).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.data_maintenance._common import dumps_canonical
from scripts.data_maintenance._static_overlays import (
    AREA_PASSTHROUGH,
    DAC1_FLOW_TYPES,
    DAC1_PRICES,
)
from scripts.data_maintenance.refresh_dac_codelists import (
    RefreshSettings,
    build_area_map,
    parse_area_codelist,
    run,
)

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


def _load_committed(name: str) -> dict[str, str]:
    """Load a committed mapping JSON file and return it as {str: str}."""
    return json.loads((_MAPPINGS / name).read_text(encoding="utf-8"))


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


_ACTIVE_COUNTRY = {
    "status": "Active",
    "code": "1",
    "name": {"narrative": ["Austria", {"xml:lang": "fr", "#text": "Autriche"}]},
    "type": "DAC member",
    "iso-alpha-3-code": "AUT",
    "dotstatcode": "AUT",
}
_ACTIVE_ORG = {
    "status": "Active",
    "code": "807",
    "name": {"narrative": ["UNEP", {"xml:lang": "fr", "#text": "PNUE"}]},
    "type": "Multilateral donor",
    "dotstatcode": "1UN016",
    # no iso-alpha-3-code — should fall back to dotstatcode
}
_INACTIVE = {
    "status": "Inactive",
    "code": "99",
    "name": {"narrative": ["Old Country"]},
    "iso-alpha-3-code": "OLD",
}
_NO_CODE = {
    "status": "Active",
    "code": "888",
    "name": {"narrative": ["No Code"]},
    # neither iso-alpha-3-code nor dotstatcode
}


# ---------------------------------------------------------------------------
# parse_area_codelist — unit tests (inline envelopes)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_parse_returns_str_str_dict() -> None:
    raw = _envelope("Providers", _ACTIVE_COUNTRY)
    result = parse_area_codelist(raw)
    assert isinstance(result, dict)
    for k, v in result.items():
        assert isinstance(k, str)
        assert isinstance(v, str)


@pytest.mark.unit
def test_parse_status_filter_case_insensitive() -> None:
    """Both "Active" (providers) and "active" (recipients) must be included."""
    raw = _envelope(
        "Mixed",
        {**_ACTIVE_COUNTRY, "status": "Active"},
        {**_ACTIVE_COUNTRY, "code": "2", "iso-alpha-3-code": "BEL", "status": "active"},
        _INACTIVE,
    )
    result = parse_area_codelist(raw)
    assert "1" in result
    assert "2" in result
    assert "99" not in result


@pytest.mark.unit
def test_parse_org_row_uses_dotstatcode() -> None:
    """Org rows (no iso3) should be projected via dotstatcode."""
    raw = _envelope("Providers", _ACTIVE_ORG)
    result = parse_area_codelist(raw)
    assert result.get("807") == "1UN016"


@pytest.mark.unit
def test_parse_dotstatcode_preferred_over_iso3() -> None:
    """When iso3 and dotstatcode diverge, the .stat code (dotstatcode) wins.

    Kosovo is the real-world case: iso3 ``XKX`` vs dotstatcode ``XKV``; the .stat
    schema value is ``XKV``, so the incoming data maps correctly.
    """
    row = {
        **_ACTIVE_COUNTRY,
        "code": "57",
        "iso-alpha-3-code": "XKX",
        "dotstatcode": "XKV",
    }
    raw = _envelope("Recipients", row)
    result = parse_area_codelist(raw)
    assert result.get("57") == "XKV"


@pytest.mark.unit
def test_parse_rows_with_no_code_field_dropped() -> None:
    """Rows with neither iso3 nor dotstatcode must be dropped."""
    raw = _envelope("Providers", _NO_CODE)
    result = parse_area_codelist(raw)
    assert "888" not in result


# ---------------------------------------------------------------------------
# parse_area_codelist — fixture-backed tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_parse_providers_fixture_shape() -> None:
    raw = _load_fixture("codelist_5.json")
    result = parse_area_codelist(raw)
    assert len(result) >= 100, f"Expected >=100 active providers, got {len(result)}"
    assert result.get("1") == "AUT", "Provider 1 should map to AUT"
    assert result.get("801") == "AUS", "Provider 801 should map to AUS"
    assert result.get("742") == "KOR", "Provider 742 should map to KOR"
    assert result.get("807") == "1UN016", "Provider 807 (UNEP) should map to 1UN016"


@pytest.mark.unit
def test_parse_recipients_fixture_shape() -> None:
    raw = _load_fixture("codelist_13.json")
    result = parse_area_codelist(raw)
    assert len(result) >= 100, f"Expected >=100 active recipients, got {len(result)}"


# ---------------------------------------------------------------------------
# build_area_map — unit tests (inline envelopes)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_build_area_map_dac1_int_sorted_passthrough_last() -> None:
    """dumps_canonical of build_area_map output must be digit-sorted with (.*) last."""
    raw5 = _envelope("Providers", _ACTIVE_COUNTRY, _ACTIVE_ORG)
    raw13 = _envelope(
        "Recipients", {**_ACTIVE_COUNTRY, "code": "55", "iso-alpha-3-code": "TUR"}
    )
    result = build_area_map(
        target="dac1", raw_by_id={"5": raw5, "13": raw13}, committed={}
    )
    # build_area_map returns raw digit-keyed dict; dumps_canonical adds passthrough
    rendered = json.loads(dumps_canonical(result))
    keys = list(rendered.keys())
    assert keys[-1] == "(.*)", "Passthrough must be the last key"
    digit_keys = [k for k in keys if k != "(.*)"]
    assert digit_keys == sorted(digit_keys, key=int), "Digit keys must be int-sorted"


@pytest.mark.unit
def test_build_area_map_providers_win_intra_live_collision() -> None:
    """When the same code appears in both live codelists, the provider value wins."""
    shared_code = "55"
    raw5 = _envelope(
        "Providers",
        {**_ACTIVE_COUNTRY, "code": shared_code, "dotstatcode": "FROM5"},
    )
    raw13 = _envelope(
        "Recipients",
        {**_ACTIVE_COUNTRY, "code": shared_code, "dotstatcode": "FROM13"},
    )
    result = build_area_map(
        target="dac2", raw_by_id={"5": raw5, "13": raw13}, committed={}
    )
    assert result[shared_code] == "FROM5", (
        "Providers (codelist 5) must win on intra-live collision"
    )


@pytest.mark.unit
def test_build_area_map_live_wins_over_committed_on_conflict() -> None:
    """When live and committed disagree on a code's value, the live value wins.

    Modelled on EU Institutions (918): the committed base carries a stale .stat
    code that the live codelist has since superseded.
    """
    code = "918"
    raw5 = _envelope(
        "Providers",
        {**_ACTIVE_COUNTRY, "code": code, "dotstatcode": "4EU001", "status": "active"},
    )
    raw13 = _envelope(
        "Recipients", {**_ACTIVE_COUNTRY, "code": "55", "dotstatcode": "TUR"}
    )
    committed = {code: "4EU003"}  # stale committed value
    result = build_area_map(
        target="dac1", raw_by_id={"5": raw5, "13": raw13}, committed=committed
    )
    assert result[code] == "4EU001", (
        "Live value must win over the stale committed value"
    )


@pytest.mark.unit
def test_build_area_map_historical_codes_preserved() -> None:
    """Historical committed codes absent from live must appear in the proposed map."""
    historical_code = "376"  # Anguilla — not in current live OECD lists
    raw5 = _envelope("Providers", _ACTIVE_COUNTRY)
    raw13 = _envelope(
        "Recipients", {**_ACTIVE_COUNTRY, "code": "55", "iso-alpha-3-code": "TUR"}
    )
    committed = {"1": "AUT", historical_code: "AIA"}
    result = build_area_map(
        target="dac1", raw_by_id={"5": raw5, "13": raw13}, committed=committed
    )
    assert result.get(historical_code) == "AIA", (
        f"Historical code {historical_code} must be preserved from committed"
    )


@pytest.mark.unit
def test_build_area_map_unknown_target_raises() -> None:
    with pytest.raises(ValueError, match="unknown target"):
        build_area_map(target="bogus", raw_by_id={}, committed={})


# ---------------------------------------------------------------------------
# build_area_map — fixture-backed additive-superset gate tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_build_area_map_dac1_superset_of_committed() -> None:
    """dac1 result must be a superset of every committed digit code (additive guarantee)."""
    raw5 = _load_fixture("codelist_5.json")
    raw13 = _load_fixture("codelist_13.json")
    raw_by_id = {"5": raw5, "13": raw13}
    committed_full = _load_committed("dac1_codes_area.json")
    committed_digits = {k: v for k, v in committed_full.items() if k.isdigit()}

    result = build_area_map(
        target="dac1", raw_by_id=raw_by_id, committed=committed_digits
    )
    result_digits = {k: v for k, v in result.items() if k.isdigit()}

    missing = [k for k in committed_digits if k not in result_digits]
    assert not missing, (
        f"build_area_map(dac1) dropped {len(missing)} committed historical codes: "
        f"{missing[:10]}{'...' if len(missing) > 10 else ''}"
    )


@pytest.mark.unit
def test_build_area_map_dac2_superset_of_committed() -> None:
    """dac2 result must be a superset of every committed digit code (additive guarantee)."""
    raw5 = _load_fixture("codelist_5.json")
    raw13 = _load_fixture("codelist_13.json")
    raw_by_id = {"5": raw5, "13": raw13}
    committed_full = _load_committed("dac2_codes_area.json")
    committed_digits = {k: v for k, v in committed_full.items() if k.isdigit()}

    result = build_area_map(
        target="dac2", raw_by_id=raw_by_id, committed=committed_digits
    )
    result_digits = {k: v for k, v in result.items() if k.isdigit()}

    missing = [k for k in committed_digits if k not in result_digits]
    assert not missing, (
        f"build_area_map(dac2) dropped {len(missing)} committed historical codes: "
        f"{missing[:10]}{'...' if len(missing) > 10 else ''}"
    )


@pytest.mark.unit
def test_build_area_map_dac1_kosovo_stays_xkv_no_false_conflict() -> None:
    """Kosovo (57) must stay XKV — dotstatcode preference avoids a spurious XKX change.

    The live codelist carries iso3 ``XKX`` and dotstatcode ``XKV`` for Kosovo;
    preferring dotstatcode keeps the value identical to committed, so the refresh
    does NOT propose a (breaking) change to the .stat code.
    """
    raw5 = _load_fixture("codelist_5.json")
    raw13 = _load_fixture("codelist_13.json")
    committed_full = _load_committed("dac1_codes_area.json")
    committed_digits = {k: v for k, v in committed_full.items() if k.isdigit()}

    result = build_area_map(
        target="dac1", raw_by_id={"5": raw5, "13": raw13}, committed=committed_digits
    )
    assert committed_digits.get("57") == "XKV", "Test pre-condition: committed has XKV"
    assert result.get("57") == "XKV", (
        f"Code 57 (Kosovo) should remain XKV, got {result.get('57')!r}"
    )


@pytest.mark.unit
def test_build_area_map_dac1_no_value_conflicts_vs_committed() -> None:
    """A refreshed committed file must be value-stable: build introduces no changes.

    After the refresh is applied, every committed code's value matches the live
    .stat code (e.g. EU Institutions 918 → 4EU001), so a re-run proposes no value
    changes — only the additive guarantee remains. Guards against a regression that
    reintroduces a spurious change (the Kosovo dotstatcode trap).
    """
    raw5 = _load_fixture("codelist_5.json")
    raw13 = _load_fixture("codelist_13.json")
    committed_full = _load_committed("dac1_codes_area.json")
    committed_digits = {k: v for k, v in committed_full.items() if k.isdigit()}

    result = build_area_map(
        target="dac1", raw_by_id={"5": raw5, "13": raw13}, committed=committed_digits
    )
    changed = {
        k: (committed_digits[k], result[k])
        for k in committed_digits
        if k in result and committed_digits[k] != result[k]
    }
    assert not changed, f"Refreshed file should have no value changes, got {changed}"
    assert committed_digits.get("918") == "4EU001"
    assert committed_digits.get("57") == "XKV"


@pytest.mark.unit
def test_build_area_map_dac1_spot_values_and_sort() -> None:
    """Spot-check key spot values, sort order, and passthrough sentinel in rendered output."""
    raw5 = _load_fixture("codelist_5.json")
    raw13 = _load_fixture("codelist_13.json")
    raw_by_id = {"5": raw5, "13": raw13}
    committed_full = _load_committed("dac1_codes_area.json")
    committed_digits = {k: v for k, v in committed_full.items() if k.isdigit()}

    result = build_area_map(
        target="dac1", raw_by_id=raw_by_id, committed=committed_digits
    )
    assert result.get("801") == "AUS"
    assert result.get("1") == "AUT"
    assert result.get("742") == "KOR"
    assert result.get("807") == "1UN016"
    # dumps_canonical unconditionally appends (.*)
    rendered = json.loads(dumps_canonical(result))
    assert rendered.get("(.*)") == "\\1", "Passthrough sentinel must be present"
    keys = list(rendered.keys())
    assert keys[-1] == "(.*)"
    digit_keys = [k for k in keys if k != "(.*)"]
    assert digit_keys == sorted(digit_keys, key=int)


# ---------------------------------------------------------------------------
# dumps_canonical — renderer tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_dumps_canonical_int_sorted() -> None:
    """Digit keys must be int-sorted ascending; passthrough always appended last."""
    mapping = {"10": "X", "2": "Y", "1": "A"}
    result = dumps_canonical(mapping)
    parsed = json.loads(result)
    keys = list(parsed.keys())
    # dumps_canonical unconditionally appends (.*)
    assert keys == ["1", "2", "10", "(.*)"]


@pytest.mark.unit
def test_dumps_canonical_passthrough_last() -> None:
    mapping = {"10": "X", "(.*)": "\\1", "1": "A"}
    result = dumps_canonical(mapping)
    keys = list(json.loads(result).keys())
    assert keys[-1] == "(.*)"


@pytest.mark.unit
def test_dumps_canonical_trailing_newline() -> None:
    result = dumps_canonical({"1": "A"})
    assert result.endswith("\n")


@pytest.mark.unit
def test_dumps_canonical_ensure_ascii() -> None:
    result = dumps_canonical({"1": "Ä"})
    # Non-ASCII characters must be escaped
    assert "\\u" in result


# ---------------------------------------------------------------------------
# Static-constant guard (G4)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_static_prices_matches_committed_file() -> None:
    """DAC1_PRICES must equal dac1_codes_prices.json."""
    path = _MAPPINGS / "dac1_codes_prices.json"
    committed = json.loads(path.read_text(encoding="utf-8"))
    assert committed == DAC1_PRICES, (
        f"DAC1_PRICES diverges from committed file.\n"
        f"  constant: {DAC1_PRICES}\n"
        f"  file:     {committed}"
    )


@pytest.mark.unit
def test_static_flow_types_matches_committed_file() -> None:
    """DAC1_FLOW_TYPES must equal dac1_codes_flow_types.json."""
    path = _MAPPINGS / "dac1_codes_flow_types.json"
    committed = json.loads(path.read_text(encoding="utf-8"))
    assert committed == DAC1_FLOW_TYPES, (
        f"DAC1_FLOW_TYPES diverges from committed file.\n"
        f"  constant: {DAC1_FLOW_TYPES}\n"
        f"  file:     {committed}"
    )


@pytest.mark.unit
def test_area_passthrough_in_committed_area_file() -> None:
    """AREA_PASSTHROUGH sentinel must appear as the last entry of dac1_codes_area.json."""
    path = _MAPPINGS / "dac1_codes_area.json"
    committed = json.loads(path.read_text(encoding="utf-8"))
    last_key = list(committed.keys())[-1]
    last_val = list(committed.values())[-1]
    assert last_key == "(.*)", (
        f"Last key in dac1_codes_area.json should be '(.*)', got {last_key!r}"
    )
    assert last_val == AREA_PASSTHROUGH["(.*)"], (
        f"Last value should be {AREA_PASSTHROUGH['(.*)']!r}, got {last_val!r}"
    )


# ---------------------------------------------------------------------------
# Diff / write tests (monkeypatched, no network)
# ---------------------------------------------------------------------------


def _mock_fetch_codelist(
    codelist_id: str, *, standard: str = "0", timeout: int = 30
) -> bytes:
    """Return minimal inline envelopes for both codelists."""
    items: dict[str, list[dict]] = {
        "5": [_ACTIVE_COUNTRY, _ACTIVE_ORG],
        "13": [
            {
                **_ACTIVE_COUNTRY,
                "code": "55",
                "iso-alpha-3-code": "TUR",
                "dotstatcode": "TUR",
            },
        ],
    }
    return _envelope("Test", *items.get(codelist_id, []))


@pytest.mark.unit
def test_run_prints_diff_when_different(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    """run() should print a unified diff when proposed != existing."""
    import scripts.data_maintenance.refresh_dac_codelists as mod

    fake_mappings = tmp_path / "mappings"
    fake_mappings.mkdir()
    # Start with a committed file that differs from what the mock fetch would produce
    (fake_mappings / "dac1_codes_area.json").write_text(
        '{"1": "OLD"}\n', encoding="utf-8"
    )

    monkeypatch.setattr(mod.ImporterPaths, "mappings", fake_mappings)
    monkeypatch.setattr(mod, "fetch_codelist_json", _mock_fetch_codelist)

    rc = run(settings=RefreshSettings(target="dac1"))
    assert rc == 0

    captured = capsys.readouterr()
    assert "--- dac1_codes_area.json (current)" in captured.out
    assert "+++ dac1_codes_area.json (proposed)" in captured.out
    # File must NOT be written in diff-only mode
    assert (fake_mappings / "dac1_codes_area.json").read_text() == '{"1": "OLD"}\n'


@pytest.mark.unit
def test_run_no_changes_when_identical(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    """run() should report 'No changes detected.' when proposed == existing."""
    import scripts.data_maintenance.refresh_dac_codelists as mod

    monkeypatch.setattr(mod, "fetch_codelist_json", _mock_fetch_codelist)

    fake_mappings = tmp_path / "mappings"
    fake_mappings.mkdir()

    # Build what the tool would write for this mock:
    # committed = {"1": "AUT"} (from mock codelist 5), live also yields "1"→"AUT",
    # so proposed = dumps_canonical({"1": "AUT", "55": "TUR", "807": "1UN016"})
    committed_digits = {"1": "AUT"}
    live5 = parse_area_codelist(_mock_fetch_codelist("5"))
    live13 = parse_area_codelist(_mock_fetch_codelist("13"))
    live_union = dict(live5)
    for k, v in live13.items():
        live_union.setdefault(k, v)
    merged = dict(committed_digits)
    merged.update(live_union)
    proposed_text = dumps_canonical(merged)
    (fake_mappings / "dac1_codes_area.json").write_text(proposed_text, encoding="utf-8")

    monkeypatch.setattr(mod.ImporterPaths, "mappings", fake_mappings)

    rc = run(settings=RefreshSettings(target="dac1"))
    assert rc == 0

    captured = capsys.readouterr()
    assert captured.out == "", "No diff output expected when identical"
    assert "No changes detected." in captured.err


@pytest.mark.unit
def test_run_check_returns_1_on_drift(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """--check must exit non-zero when the live source differs from committed."""
    import scripts.data_maintenance.refresh_dac_codelists as mod

    fake_mappings = tmp_path / "mappings"
    fake_mappings.mkdir()
    (fake_mappings / "dac1_codes_area.json").write_text(
        '{"1": "OLD"}\n', encoding="utf-8"
    )
    monkeypatch.setattr(mod.ImporterPaths, "mappings", fake_mappings)
    monkeypatch.setattr(mod, "fetch_codelist_json", _mock_fetch_codelist)

    rc = run(settings=RefreshSettings(target="dac1", check=True))
    assert rc == 1, "--check must return 1 when committed is out of date"
    # diff-only: the tracked file must be left untouched
    assert (fake_mappings / "dac1_codes_area.json").read_text() == '{"1": "OLD"}\n'


@pytest.mark.unit
def test_run_check_returns_0_when_in_sync(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """--check must exit zero when committed already matches the live source."""
    import scripts.data_maintenance.refresh_dac_codelists as mod

    monkeypatch.setattr(mod, "fetch_codelist_json", _mock_fetch_codelist)
    fake_mappings = tmp_path / "mappings"
    fake_mappings.mkdir()

    live5 = parse_area_codelist(_mock_fetch_codelist("5"))
    live13 = parse_area_codelist(_mock_fetch_codelist("13"))
    live_union = dict(live5)
    for k, v in live13.items():
        live_union.setdefault(k, v)
    (fake_mappings / "dac1_codes_area.json").write_text(
        dumps_canonical(live_union), encoding="utf-8"
    )
    monkeypatch.setattr(mod.ImporterPaths, "mappings", fake_mappings)

    rc = run(settings=RefreshSettings(target="dac1", check=True))
    assert rc == 0, "--check must return 0 when committed is in sync"


@pytest.mark.unit
def test_run_write_updates_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    """--write should overwrite the area file with the canonical proposal."""
    import scripts.data_maintenance.refresh_dac_codelists as mod

    monkeypatch.setattr(mod, "fetch_codelist_json", _mock_fetch_codelist)
    monkeypatch.setattr(mod, "_PROVENANCE_PATH", tmp_path / "_provenance.json")

    fake_mappings = tmp_path / "mappings"
    fake_mappings.mkdir()
    (fake_mappings / "dac1_codes_area.json").write_text(
        '{"1": "OLD"}\n', encoding="utf-8"
    )

    monkeypatch.setattr(mod.ImporterPaths, "mappings", fake_mappings)

    rc = run(settings=RefreshSettings(target="dac1", write=True))
    assert rc == 0

    written = json.loads((fake_mappings / "dac1_codes_area.json").read_text())
    # "1" comes from live (AUT) which overlays committed "OLD"
    assert written.get("1") == "AUT"
    # Historical committed code "1" (now replaced by live "AUT") is present
    assert list(written.keys())[-1] == "(.*)"


@pytest.mark.unit
def test_run_unknown_target_returns_error(capsys: pytest.CaptureFixture) -> None:
    """An unknown --target must return exit code 1."""
    rc = run(settings=RefreshSettings(target="bogus"))
    assert rc == 1
    captured = capsys.readouterr()
    assert "error" in captured.err.lower()
