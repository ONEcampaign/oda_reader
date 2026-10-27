"""Tests for converting API area codes to .Stat codes.

The rule, the lookup and the converters are tested with small synthetic
mappings, so the tests don't depend on today's OECD codes. The one exception is
`test_shipped_mappings_resolve`, which reads the real mapping files and asserts
only that they build a lookup without a clash.
"""

from __future__ import annotations

import logging

import pandas as pd
import pytest

from oda_reader.schemas import (
    crs_translation,
    dac1_translation,
    dac2_translation,
    multisystem_translation,
)
from oda_reader.schemas.schema_tools import (
    build_area_code_lookup,
    find_area_code_clashes,
    map_area_codes,
)


def _codes(values: list[str | None]) -> pd.Series:
    return pd.Series(pd.array(values, dtype="string[pyarrow]"))


# ---------------------------------------------------------------------------
# The rule
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_api_code_with_one_dotstat_code_passes_through() -> None:
    lookup = build_area_code_lookup({1: "AAA", 2: "BBB"}, corrections={})
    assert lookup == {"AAA": 1, "BBB": 2}


@pytest.mark.unit
@pytest.mark.parametrize(
    "mapping",
    [{10: "AAA", 20: "AAA", 3: "BBB"}, {20: "AAA", 10: "AAA", 3: "BBB"}],
    ids=["correction-first", "correction-last"],
)
def test_correction_picks_among_shared_codes_in_any_order(
    mapping: dict[int, str],
) -> None:
    lookup = build_area_code_lookup(mapping, corrections={10: "AAA"})
    assert lookup == {"AAA": 10, "BBB": 3}


@pytest.mark.unit
def test_correction_can_name_a_code_only_it_has() -> None:
    # The corrections are merged into the mapping, as area_code_mapping() does.
    base = {1: "AAA"}
    corrections = {100: "AAA"}
    lookup = build_area_code_lookup(base | corrections, corrections=corrections)
    assert lookup == {"AAA": 100}


@pytest.mark.unit
def test_shared_codes_without_a_correction_clash() -> None:
    mapping = {10: "AAA", 20: "AAA", 3: "BBB"}

    assert find_area_code_clashes(mapping, corrections={}) == {"AAA": [10, 20]}
    with pytest.raises(ValueError, match=r"AAA -> \[10, 20\]"):
        build_area_code_lookup(mapping, corrections={})


@pytest.mark.unit
def test_two_corrections_for_one_api_code_clash() -> None:
    corrections = {10: "AAA", 20: "AAA"}
    mapping = {30: "AAA"} | corrections

    assert find_area_code_clashes(mapping, corrections) == {"AAA": [10, 20, 30]}


@pytest.mark.unit
def test_no_clashes_returns_empty() -> None:
    assert find_area_code_clashes({1: "AAA", 2: "BBB"}, corrections={}) == {}


# ---------------------------------------------------------------------------
# map_area_codes
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_map_area_codes_maps_through_lookup() -> None:
    df = pd.DataFrame({"donor_code": _codes(["AAA", "BBB"])})

    result = map_area_codes(df, area_code_lookup={"AAA": 1, "BBB": 2})

    assert result["donor_code"].tolist() == [1, 2]
    assert str(result["donor_code"].dtype) == "int32[pyarrow]"


@pytest.mark.unit
def test_map_area_codes_warns_about_unknown_codes(
    caplog: pytest.LogCaptureFixture,
) -> None:
    df = pd.DataFrame({"donor_code": _codes(["AAA", "ZZZ", "ZZZ"])})

    with caplog.at_level(logging.WARNING, logger="oda_importer"):
        result = map_area_codes(df, area_code_lookup={"AAA": 1})

    assert result["donor_code"].tolist() == [1, pd.NA, pd.NA]
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "2 rows in 'donor_code'" in warnings[0].message
    assert "['ZZZ']" in warnings[0].message


@pytest.mark.unit
def test_map_area_codes_does_not_warn_about_missing_values(
    caplog: pytest.LogCaptureFixture,
) -> None:
    df = pd.DataFrame({"donor_code": _codes(["AAA", None])})

    with caplog.at_level(logging.WARNING, logger="oda_importer"):
        result = map_area_codes(df, area_code_lookup={"AAA": 1})

    assert result["donor_code"].tolist() == [1, pd.NA]
    assert not [r for r in caplog.records if r.levelno == logging.WARNING]


@pytest.mark.unit
def test_map_area_codes_maps_a_separate_target_column() -> None:
    df = pd.DataFrame({"recipient_code": _codes(["AAA"])})

    result = map_area_codes(
        df,
        area_code_lookup={"AAA": 1},
        source_column="recipient_code",
        target_column="recipient_dotstat",
    )

    assert result["recipient_code"].tolist() == ["AAA"]
    assert result["recipient_dotstat"].tolist() == [1]


@pytest.mark.unit
def test_map_area_codes_rejects_the_old_keyword() -> None:
    df = pd.DataFrame({"donor_code": _codes(["AAA"])})

    with pytest.raises(TypeError):
        map_area_codes(df, area_code_mapping={1: "AAA"})  # type: ignore[call-arg]


@pytest.mark.unit
def test_map_area_codes_rejects_an_old_mapping_passed_positionally() -> None:
    df = pd.DataFrame({"donor_code": _codes(["AAA"])})

    with pytest.raises(TypeError, match="must map API codes"):
        map_area_codes(df, {1: "AAA"})  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Converters use the lookup for donor and recipient columns
# ---------------------------------------------------------------------------

_LOOKUP = {"AAA": 1, "BBB": 2}
_PRICES = {"XX": "YY"}


@pytest.mark.unit
def test_dac1_converter_maps_donors(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(dac1_translation, "area_code_lookup", lambda: _LOOKUP)
    monkeypatch.setattr(dac1_translation, "prices_mapping", lambda: _PRICES)
    df = pd.DataFrame(
        {
            "donor_code": _codes(["AAA", "BBB"]),
            "unit_measure_code": ["USD", "USD"],
            "unit_measure_name": ["US dollar", "US dollar"],
            "amounttype_code": ["XX", "XX"],
            "amount_type": ["Current prices", "Current prices"],
        }
    )

    result = dac1_translation.convert_dac1_to_dotstat_codes(df)

    assert sorted(result["donor_code"].tolist()) == [1, 2]


@pytest.mark.unit
@pytest.mark.parametrize(
    ("module", "convert"),
    [
        (dac2_translation, dac2_translation.convert_dac2_to_dotstat_codes),
        (crs_translation, crs_translation.convert_crs_to_dotstat_codes),
        (
            multisystem_translation,
            multisystem_translation.convert_multisystem_to_dotstat_codes,
        ),
    ],
    ids=["dac2", "crs", "multisystem"],
)
def test_converter_maps_donors_and_recipients(
    monkeypatch: pytest.MonkeyPatch, module, convert
) -> None:
    monkeypatch.setattr(module, "area_code_lookup", lambda: _LOOKUP)
    monkeypatch.setattr(module, "prices_mapping", lambda: _PRICES)
    df = pd.DataFrame(
        {
            "donor_code": _codes(["AAA", "BBB"]),
            "recipient_code": _codes(["BBB", "AAA"]),
            "data_type_code": ["XX", "XX"],
        }
    )

    result = convert(df)

    assert result["donor_code"].tolist() == [1, 2]
    assert result["recipient_code"].tolist() == [2, 1]


# ---------------------------------------------------------------------------
# The shipped mapping files
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.parametrize(
    "module",
    [dac1_translation, dac2_translation, crs_translation],
    ids=["dac1", "dac2", "crs"],
)
def test_shipped_mappings_resolve(module) -> None:
    """The committed mapping files build a lookup without a clash.

    A clash makes every converted download fail, so this guards against an
    edit to the mapping files (such as removing a correction that looks
    redundant) that leaves an API code with no single .Stat code. It asserts
    no particular codes.
    """
    module.area_code_lookup()
