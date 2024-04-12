import pandas as pd

from oda_data import config

from oda_data.logger import logger

from oda_importer.common import df_from_api
from oda_importer.schemas.dac1_translation import area_code_mapping, prices_mapping
from oda_importer.schemas.schema_tools import (
    dac1_schema_translation,
    get_dtypes,
    get_columns_to_keep,
    get_column_name_mapping,
)

DAC1_API_ENDPOINT: str = (
    "https://sdmx.oecd.org/public/rest/data/"
    "OECD.DCD.FSD,DSD_DAC1@DF_DAC1,1.1/"
    "all?dimensionAtObservation=AllDimensions&format=csvfilewithlabels&startPeriod=2022"
)


def download_dac1() -> pd.DataFrame:

    # Load the translation schema from .stat  to the new explorer
    schema_translation = dac1_schema_translation()

    # Get a data types dictionary
    data_types = get_dtypes(schema=schema_translation)

    # Set read csv options
    df_options = {
        "na_values": ("_Z", "nan"),
        "keep_default_na": True,
        "dtype": data_types,
    }

    # Get the dataframe
    df = df_from_api(url=DAC1_API_ENDPOINT, read_csv_options=df_options)

    # Get columns to keep
    to_keep = get_columns_to_keep(schema=schema_translation)

    # keep only selected columns
    df = df.filter(items=to_keep)

    # Get column name mapping
    name_mapping = get_column_name_mapping(schema=schema_translation)

    # rename columns
    df = df.rename(columns=name_mapping)

    # Map old codes
    donor_codes = {v: k for k, v in area_code_mapping().items()}
    aidtype_codes = prices_mapping()

    # Map donor_codes
    df["donor_code"] = df["donor_code"].map(donor_codes).astype("int32[pyarrow]")

    # Map aidtypes
    df["aidtype_code"] = df["aidtype_code"].map(aidtype_codes)

    return df


if __name__ == "__main__":
    data = download_dac1()
