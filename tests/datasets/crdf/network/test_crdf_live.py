"""Live checks against the CRDF folder at https://webfs.oecd.org/climate/.

Downloads every workbook in the folder (about 250 MB on a cold cache) into the
package's CRDF cache. Set ``RUN_NETWORK_TESTS=1`` to opt in.
"""

import os

import pytest

from oda_reader import crdf
from oda_reader.crdf import download_crdf, list_crdf_files

pytestmark = [pytest.mark.network, pytest.mark.integration]


@pytest.fixture(autouse=True)
def _require_network():
    if os.environ.get("RUN_NETWORK_TESTS") != "1":
        pytest.skip("set RUN_NETWORK_TESTS=1 to run network tests")


def test_every_live_header_maps():
    """Every data sheet in every perspective workbook matches a known layout."""
    listing = list_crdf_files()
    workbooks = listing.loc[listing["perspective"].notna()]
    assert set(workbooks["perspective"]) == {"provider", "recipient"}

    for name, perspective, url in zip(
        workbooks["name"], workbooks["perspective"], workbooks["url"], strict=True
    ):
        path, etag = crdf._fetch(url)
        assert etag, name
        _, sheets = crdf._read_headers(path)
        assert sheets, name
        for sheet, headers in sheets.items():
            layout, _, price_year = crdf._match_layout(
                headers, source=name, sheet=sheet
            )
            assert crdf._schema()["layouts"][layout]["perspective"] == perspective
            print(f"{name} [{sheet}]: {layout}, price year {price_year}")


@pytest.mark.parametrize("perspective", ["provider", "recipient"])
def test_reads_2015_to_2024(perspective):
    df = download_crdf(perspective, start_year=2015, end_year=2024)

    assert sorted(df["year"].unique()) == list(range(2015, 2025))
    for column in ("price_year", "vintage", "source_file", "file_etag"):
        assert df[column].notna().all(), column
    counts = df.groupby(["source_file", "vintage", "price_year"]).size()
    print(counts.to_string())
