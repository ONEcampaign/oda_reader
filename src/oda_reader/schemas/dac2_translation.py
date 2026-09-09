import warnings

import pandas as pd

from oda_reader.common import ImporterPaths
from oda_reader.schemas.dac1_translation import prices_mapping
from oda_reader.schemas.schema_tools import map_amount_type_codes, map_area_codes
from oda_reader.schemas.xml_tools import read_mapping

MAPPINGS = {
    "dac2_codes_area": ImporterPaths.mappings / "dac2_codes_area.json",
    "area_code_corrections": ImporterPaths.mappings / "area_code_corrections.json",
    "dac2b_measure_dotstat": ImporterPaths.mappings / "dac2b_measure_dotstat.json",
}


def area_code_mapping() -> dict:
    """Reads the area code mapping."""
    return read_mapping(
        MAPPINGS["dac2_codes_area"],
        keys_as_int=True,
    ) | read_mapping(
        MAPPINGS["area_code_corrections"],
        keys_as_int=True,
    )


def convert_dac2_to_dotstat_codes(df: pd.DataFrame) -> pd.DataFrame:
    """Convert the DAC2 data (DAC2A or DAC2B) to the .stat schema.

    It maps donor/recipient area codes and the price base only, never
    `MEASURE`, so it is DAC2-generic and serves both tables unchanged.

    Args:
        df: The DAC2A or DAC2B data.

    Returns:
        The data in the .stat schema.
    """
    area_codes = area_code_mapping()
    prices_codes = prices_mapping()

    df = map_area_codes(df, area_code_mapping=area_codes)

    df = map_area_codes(
        df,
        area_code_mapping=area_codes,
        source_column="recipient_code",
        target_column="recipient_code",
    )

    df = map_amount_type_codes(
        df,
        prices_mapping=prices_codes,
        source_column="data_type_code",
        target_column="data_type_code",
    )

    return df


def dac2b_measure_mapping() -> dict:
    """Reads the DAC2B API MEASURE -> .stat AIDTYPE mapping."""
    return read_mapping(
        MAPPINGS["dac2b_measure_dotstat"],
        keys_as_int=True,
    )


def convert_dac2b_to_dotstat_codes(df: pd.DataFrame) -> pd.DataFrame:
    """Convert the DAC2B data to the .stat schema.

    DAC2B's API `MEASURE` codes are not already in .stat numbering, unlike
    DAC2A's. This runs the DAC2-generic conversion first, then remaps
    `aidtype_code` (already renamed from `MEASURE` by `preprocess`) from API
    numbering to .stat numbering, using the explicit mapping in
    `dac2b_measure_dotstat.json`.

    Args:
        df: The DAC2B data, with `aidtype_code` holding raw API MEASURE
            codes.

    Returns:
        pd.DataFrame: The data in the .stat schema.

    Raises:
        ValueError: If `aidtype_code` contains a code absent from
            `dac2b_measure_dotstat.json`.
    """
    df = convert_dac2_to_dotstat_codes(df)

    mapping = dac2b_measure_mapping()

    codes = set(df["aidtype_code"].unique().tolist())
    unmapped = codes - set(mapping)
    if unmapped:
        raise ValueError(
            f"Unmapped DAC2B measure code(s): {sorted(unmapped)}. Add them to "
            f"{MAPPINGS['dac2b_measure_dotstat']}."
        )

    df["aidtype_code"] = df["aidtype_code"].map(mapping).astype("int32[pyarrow]")

    return df


def convert_dac2a_to_dotstat_codes(df: pd.DataFrame) -> pd.DataFrame:
    """Deprecated alias for `convert_dac2_to_dotstat_codes`.

    Kept so existing code built against the DAC2A-only name keeps working.
    `convert_dac2_to_dotstat_codes` is the DAC2-generic name and also serves
    DAC2B.
    """
    warnings.warn(
        "convert_dac2a_to_dotstat_codes is deprecated. Use "
        "convert_dac2_to_dotstat_codes instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    return convert_dac2_to_dotstat_codes(df)
