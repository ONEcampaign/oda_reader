"""Unit tests for CRS bulk-download URL resolution and dispatch.

Covers `download_crs_file`, `get_year_crs_zip_url`/`get_year_crs_zip_id`,
and the parquet-bulk (`bulk_download_crs`) siblings.
"""

import pandas as pd
import pytest

from oda_reader.crs import (
    CRS_FLOW_URL,
    bulk_download_crs,
    download_crs_file,
    get_full_crs_parquet_id,
    get_full_crs_parquet_url,
    get_reduced_crs_parquet_id,
    get_reduced_crs_parquet_url,
    get_year_crs_zip_id,
    get_year_crs_zip_url,
)

_YEAR_ZIP_URL = "https://webfs-dcd.oecd.org/files/dotStat/DSD_CRS/CRS 2022 data.zip"
_FULL_PARQUET_URL = "https://webfs-dcd.oecd.org/files/dotStat/DSD_CRS/CRS.parquet"
_REDUCED_PARQUET_URL = (
    "https://webfs-dcd.oecd.org/files/dotStat/DSD_CRS/CRS-reduced.parquet"
)


@pytest.mark.unit
class TestCRSUrlHelpers:
    def test_get_year_crs_zip_url_calls_get_bulk_file_url(self, mocker):
        mock = mocker.patch(
            "oda_reader.crs.get_bulk_file_url", return_value=_YEAR_ZIP_URL
        )

        result = get_year_crs_zip_url(2022)

        assert result == _YEAR_ZIP_URL
        mock.assert_called_once_with(
            flow_url=CRS_FLOW_URL, label="CRS 2022 (dotStat format)"
        )

    def test_get_year_crs_zip_id_is_deprecated_and_delegates(self, mocker):
        mocker.patch("oda_reader.crs.get_bulk_file_url", return_value=_YEAR_ZIP_URL)

        with pytest.warns(DeprecationWarning, match="get_year_crs_zip_url"):
            result = get_year_crs_zip_id(2022)

        assert result == _YEAR_ZIP_URL

    def test_get_full_crs_parquet_url(self, mocker):
        mock = mocker.patch(
            "oda_reader.crs.get_bulk_file_url", return_value=_FULL_PARQUET_URL
        )

        assert get_full_crs_parquet_url() == _FULL_PARQUET_URL
        mock.assert_called_once_with(flow_url=CRS_FLOW_URL, label="CRS-Parquet")

    def test_get_full_crs_parquet_id_is_deprecated_and_delegates(self, mocker):
        mocker.patch("oda_reader.crs.get_bulk_file_url", return_value=_FULL_PARQUET_URL)

        with pytest.warns(DeprecationWarning, match="get_full_crs_parquet_url"):
            result = get_full_crs_parquet_id()

        assert result == _FULL_PARQUET_URL

    def test_get_reduced_crs_parquet_url(self, mocker):
        mock = mocker.patch(
            "oda_reader.crs.get_bulk_file_url", return_value=_REDUCED_PARQUET_URL
        )

        assert get_reduced_crs_parquet_url() == _REDUCED_PARQUET_URL
        mock.assert_called_once_with(flow_url=CRS_FLOW_URL, label="CRS-reduced-parquet")

    def test_get_reduced_crs_parquet_id_is_deprecated_and_delegates(self, mocker):
        mocker.patch(
            "oda_reader.crs.get_bulk_file_url", return_value=_REDUCED_PARQUET_URL
        )

        with pytest.warns(DeprecationWarning, match="get_reduced_crs_parquet_url"):
            result = get_reduced_crs_parquet_id()

        assert result == _REDUCED_PARQUET_URL


@pytest.mark.unit
class TestDownloadCrsFile:
    """Tests for `download_crs_file`."""

    def test_resolves_url_and_dispatches_to_bulk_download_parquet(self, mocker):
        mocker.patch(
            "oda_reader.crs.get_bulk_file_url_with_version",
            return_value=(_YEAR_ZIP_URL, "v20260803"),
        )
        mock_bulk = mocker.patch(
            "oda_reader.crs.bulk_download_parquet",
            return_value=pd.DataFrame({"a": [1]}),
        )

        result = download_crs_file(2022)

        assert isinstance(result, pd.DataFrame)
        mock_bulk.assert_called_once_with(
            url=_YEAR_ZIP_URL,
            save_to_path=None,
            as_iterator=False,
            use_raw_cache=True,
            flow_url=CRS_FLOW_URL,
            label="CRS 2022 (dotStat format)",
            version="v20260803",
        )

    def test_passes_save_to_path_iterator_and_cache_flags(self, mocker, tmp_path):
        mocker.patch(
            "oda_reader.crs.get_bulk_file_url_with_version",
            return_value=(_YEAR_ZIP_URL, "v20260803"),
        )
        mock_bulk = mocker.patch(
            "oda_reader.crs.bulk_download_parquet", return_value=None
        )

        download_crs_file(
            2022, save_to_path=tmp_path, as_iterator=True, use_raw_cache=False
        )

        mock_bulk.assert_called_once_with(
            url=_YEAR_ZIP_URL,
            save_to_path=tmp_path,
            as_iterator=True,
            use_raw_cache=False,
            flow_url=CRS_FLOW_URL,
            label="CRS 2022 (dotStat format)",
            version="v20260803",
        )

    def test_year_accepts_string(self, mocker):
        """Year may be passed as a str (e.g. from user code building it dynamically)."""
        mocker.patch(
            "oda_reader.crs.get_bulk_file_url_with_version",
            return_value=(_YEAR_ZIP_URL, "v20260803"),
        )
        mock_bulk = mocker.patch(
            "oda_reader.crs.bulk_download_parquet", return_value=None
        )

        download_crs_file("2022")

        assert mock_bulk.call_args.kwargs["label"] == "CRS 2022 (dotStat format)"


@pytest.mark.unit
class TestBulkDownloadCrs:
    def test_full_version_resolves_and_dispatches(self, mocker):
        mock_url = mocker.patch(
            "oda_reader.crs.get_bulk_file_url_with_version",
            return_value=(_FULL_PARQUET_URL, "v20260803"),
        )
        mock_bulk = mocker.patch(
            "oda_reader.crs.bulk_download_parquet", return_value=None
        )

        bulk_download_crs()

        mock_url.assert_called_once_with(flow_url=CRS_FLOW_URL, label="CRS-Parquet")
        mock_bulk.assert_called_once_with(
            url=_FULL_PARQUET_URL,
            save_to_path=None,
            as_iterator=False,
            use_raw_cache=True,
            flow_url=CRS_FLOW_URL,
            label="CRS-Parquet",
            version="v20260803",
        )

    def test_reduced_version_resolves_and_dispatches(self, mocker):
        mock_url = mocker.patch(
            "oda_reader.crs.get_bulk_file_url_with_version",
            return_value=(_REDUCED_PARQUET_URL, "v20260803"),
        )
        mock_bulk = mocker.patch(
            "oda_reader.crs.bulk_download_parquet", return_value=None
        )

        bulk_download_crs(reduced_version=True)

        mock_url.assert_called_once_with(
            flow_url=CRS_FLOW_URL, label="CRS-reduced-parquet"
        )
        mock_bulk.assert_called_once_with(
            url=_REDUCED_PARQUET_URL,
            save_to_path=None,
            as_iterator=False,
            use_raw_cache=True,
            flow_url=CRS_FLOW_URL,
            label="CRS-reduced-parquet",
            version="v20260803",
        )
