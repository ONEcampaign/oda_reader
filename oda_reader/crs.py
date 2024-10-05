from pathlib import Path

from oda_reader.download.download_tools import get_bulk_file_id, bulk_download_parquet


def get_full_crs_parquet_id():
    return get_bulk_file_id(search_string="CRS-Parquet|")


def get_reduced_crs_parquet_id():
    return get_bulk_file_id(search_string="CRS-reduced-parquet|")


def get_year_crs_zip_id(year: int):
    return get_bulk_file_id(search_string=f"CRS {year} (dotStat format)|")


def download_crs(save_to_path: Path | str | None = None, reduced_version: bool = False):
    """
    Download the CRS data from the bulk download service. The file is very large.
    It is therefore strongly recommended to save it to disk. If save_to_path is not
    provided, the function will return a DataFrame.

    Args:
        save_to_path: The path to save the file to. Optional. If not provided, a
        DataFrame is returned.
        reduced_version: Whether to download the reduced version of the CRS data.

    Returns:
        pd.DataFrame | None: The DataFrame if save_to_path is not provided.

    """

    if reduced_version:
        file_id = get_reduced_crs_parquet_id()
    else:
        file_id = get_full_crs_parquet_id()

    return bulk_download_parquet(file_id=file_id, save_to_path=save_to_path)
