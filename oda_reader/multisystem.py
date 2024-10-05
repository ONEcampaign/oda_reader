from pathlib import Path

from oda_reader.download.download_tools import (
    get_bulk_file_id,
    MULTI_FLOW_URL,
    bulk_download_parquet,
)


def get_full_multisystem_id():
    return get_bulk_file_id(
        flow_url=MULTI_FLOW_URL, search_string="Entire dataset (dotStat format)|"
    )


def download_multisystem_file(save_to_path: Path | str | None = None):
    """
    Download the Multisystem data from the bulk download service. The file is very large.
    It is therefore strongly recommended to save it to disk. If save_to_path is not
    provided, the function will return a DataFrame.

    Args:
        save_to_path: The path to save the file to. Optional. If not provided, a
        DataFrame is returned.


    Returns:
        pd.DataFrame | None: The DataFrame if save_to_path is not provided.

    """

    file_id = get_full_multisystem_id()

    return bulk_download_parquet(
        file_id=file_id, save_to_path=save_to_path, is_txt=True
    )


if __name__ == "__main__":
    df = download_multisystem_file()
