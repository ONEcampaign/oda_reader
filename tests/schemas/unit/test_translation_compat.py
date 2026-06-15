"""Characterization tests for the runtime translation consumers.

These tests pin the output of area_code_mapping, prices_mapping, and the
schema_tools mapping functions so that future refactors cannot silently
change their behaviour.  They test the mapping layer directly rather than
the full convert_* pipeline (which requires a richer DataFrame structure
not relevant to this characterization).

All tests are @pytest.mark.unit and have no external dependencies.
"""

from __future__ import annotations

import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# helpers — build minimal frames for schema_tools map functions
# ---------------------------------------------------------------------------


def _area_frame(codes: list[str]) -> pd.DataFrame:
    return pd.DataFrame({"donor_code": pd.array(codes, dtype="string[pyarrow]")})


def _recipient_frame(donor: list[str], recipient: list[str]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "donor_code": pd.array(donor, dtype="string[pyarrow]"),
            "recipient_code": pd.array(recipient, dtype="string[pyarrow]"),
        }
    )


def _prices_frame(codes: list[str], col: str = "amounttype_code") -> pd.DataFrame:
    return pd.DataFrame({col: pd.array(codes, dtype="string[pyarrow]")})


# ---------------------------------------------------------------------------
# dac1_translation — mapping accessors
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_dac1_area_mapping_returns_int_keyed_dict() -> None:
    """area_code_mapping must return {int: str} with integer keys."""
    from oda_reader.schemas.dac1_translation import area_code_mapping

    m = area_code_mapping()
    assert isinstance(m, dict)
    assert len(m) > 400, f"Expected >400 entries, got {len(m)}"
    assert isinstance(next(iter(m)), int), "Keys must be integers"
    for v in m.values():
        assert isinstance(v, str)


@pytest.mark.unit
def test_dac1_area_mapping_spot_values() -> None:
    """Spot-check critical dac1 area code mappings."""
    from oda_reader.schemas.dac1_translation import area_code_mapping

    m = area_code_mapping()
    cases = {1: "AUT", 801: "AUS", 742: "KOR", 4: "FRA"}
    for dac_code, expected in cases.items():
        assert m.get(dac_code) == expected, (
            f"Expected m[{dac_code}]={expected!r}, got {m.get(dac_code)!r}"
        )


@pytest.mark.unit
def test_dac1_prices_mapping() -> None:
    """Prices mapping: V→A, Q→D."""
    from oda_reader.schemas.dac1_translation import prices_mapping

    m = prices_mapping()
    assert m["V"] == "A"
    assert m["Q"] == "D"


@pytest.mark.unit
def test_dac1_flow_types_mapping() -> None:
    """Flow types mapping must include 115→C, 112→D, and the regex passthrough."""
    from oda_reader.schemas.dac1_translation import flow_types_mapping

    m = flow_types_mapping()
    assert m["115"] == "C"
    assert m["112"] == "D"
    assert m["(.*)"] == "\\1"


# ---------------------------------------------------------------------------
# crs_translation — mapping accessors
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_crs_area_mapping_keys_are_integers() -> None:
    """CRS area_code_mapping must return int-keyed dict."""
    from oda_reader.schemas.crs_translation import area_code_mapping

    m = area_code_mapping()
    assert len(m) > 400
    for k in m:
        assert isinstance(k, int), f"Expected int key, got {type(k).__name__}: {k!r}"


@pytest.mark.unit
def test_crs_area_mapping_spot_values() -> None:
    """CRS area mapping must share dac2's area values for AUT and AUS."""
    from oda_reader.schemas.crs_translation import area_code_mapping

    m = area_code_mapping()
    assert m.get(1) == "AUT"
    assert m.get(801) == "AUS"


# ---------------------------------------------------------------------------
# schema_tools map functions — direct characterization
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_map_area_codes_inverts_mapping() -> None:
    """map_area_codes inverts {dac_int: iso3} to {iso3: dac_int} and maps the column."""
    from oda_reader.schemas.dac1_translation import area_code_mapping
    from oda_reader.schemas.schema_tools import map_area_codes

    m = area_code_mapping()
    df = _area_frame(["AUT"])
    result = map_area_codes(df, area_code_mapping=m)
    assert int(result["donor_code"].iloc[0]) == 1


@pytest.mark.unit
def test_map_area_codes_australia() -> None:
    """Australia (AUS → 801) must map correctly."""
    from oda_reader.schemas.dac1_translation import area_code_mapping
    from oda_reader.schemas.schema_tools import map_area_codes

    m = area_code_mapping()
    df = _area_frame(["AUS"])
    result = map_area_codes(df, area_code_mapping=m)
    assert int(result["donor_code"].iloc[0]) == 801


@pytest.mark.unit
def test_map_amount_type_codes_prices() -> None:
    """map_amount_type_codes applies prices_mapping directly: V→A and Q→D."""
    from oda_reader.schemas.dac1_translation import prices_mapping
    from oda_reader.schemas.schema_tools import map_amount_type_codes

    # prices_mapping = {"V": "A", "Q": "D"} — maps old DAC codes to .stat codes
    m = prices_mapping()
    df = _prices_frame(["V", "Q"])
    result = map_amount_type_codes(df, prices_mapping=m)
    assert result["amounttype_code"].iloc[0] == "A"
    assert result["amounttype_code"].iloc[1] == "D"


@pytest.mark.unit
def test_map_area_codes_recipient_column() -> None:
    """map_area_codes with source/target=recipient_code maps recipients correctly."""
    from oda_reader.schemas.crs_translation import area_code_mapping
    from oda_reader.schemas.schema_tools import map_area_codes

    m = area_code_mapping()
    df = _recipient_frame(["AUT"], ["AUS"])
    result = map_area_codes(
        df,
        area_code_mapping=m,
        source_column="recipient_code",
        target_column="recipient_code",
    )
    assert int(result["recipient_code"].iloc[0]) == 801
