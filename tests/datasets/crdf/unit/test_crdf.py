"""Unit tests for the CRDF reader.

Workbooks are built in tmp_path from tests/fixtures/crdf/layouts.json, which
holds the real header row of each known layout and two synthetic data rows.
The folder listings are snapshots of https://webfs.oecd.org/climate/.
"""

import json
from datetime import datetime
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

import pandas as pd
import pytest
from openpyxl import Workbook
from openpyxl.writer.excel import ExcelWriter

from oda_reader import crdf
from oda_reader.crdf import download_crdf, download_crdf_file, list_crdf_files
from oda_reader.exceptions import CRDFSchemaError

FIXTURES = Path(__file__).parents[3] / "fixtures" / "crdf"
LAYOUTS = json.loads((FIXTURES / "layouts.json").read_text(encoding="utf-8"))
LISTINGS = {
    crdf.CRDF_URL: "listing_root.html",
    crdf.CRDF_URL + "ProviderPerspective/": "listing_provider.html",
    crdf.CRDF_URL + "RecipientPerspective/": "listing_recipient.html",
}


def write_workbook(
    path: Path, fixture: dict, *, headers: list | None = None, rows=None
) -> Path:
    """Write a workbook with the fixture's sheets, header row and data rows."""
    workbook = Workbook()
    workbook.remove(workbook.active)
    for sheet in fixture["sheets"]:
        ws = workbook.create_sheet(sheet)
        if sheet == fixture["data_sheet"]:
            ws.append(fixture["headers"] if headers is None else headers)
            for row in fixture["rows"] if rows is None else rows:
                ws.append(row)
        else:
            ws.append(["Notes"])
    workbook.properties.modified = datetime.fromisoformat(fixture["modified"])
    # Workbook.save stamps the current time over the modified date, and the
    # reader takes the vintage from it, so write through ExcelWriter.
    with ZipFile(path, "w", ZIP_DEFLATED) as archive:
        ExcelWriter(workbook, archive).save()
    return path


@pytest.fixture
def workbook_for(tmp_path):
    def build(layout: str, **kwargs) -> Path:
        fixture = LAYOUTS[layout]
        return write_workbook(tmp_path / fixture["file"], fixture, **kwargs)

    return build


@pytest.fixture
def live_folder(monkeypatch):
    """Serve the listing snapshots and let _fetch return fixture workbooks."""

    def fake_get(url, headers, *, force_refresh=False):
        return 200, (FIXTURES / LISTINGS[url]).read_text(encoding="utf-8"), False

    monkeypatch.setattr(crdf, "get_response_text", fake_get)


def test_schema_layouts_and_fixtures_cover_each_other():
    """Every known layout has a header fixture and every fixture names a layout."""
    assert set(LAYOUTS) == set(crdf._schema()["layouts"])


@pytest.mark.parametrize("layout", sorted(LAYOUTS))
def test_fixture_headers_map_with_no_gaps(layout, workbook_for):
    fixture = LAYOUTS[layout]
    path = workbook_for(layout)

    df = download_crdf(fixture["perspective"], file=path)

    mapped = set(crdf._schema()["layouts"][layout]["columns"].values())
    assert set(df.columns) == mapped | {
        "price_year",
        "vintage",
        "source_file",
        "file_etag",
    }
    assert len(df) == 2
    assert df["price_year"].tolist() == [fixture["price_year"]] * 2
    assert (
        df["vintage"].tolist()
        == [datetime.fromisoformat(fixture["modified"]).date()] * 2
    )
    assert df["source_file"].tolist() == [fixture["file"]] * 2
    assert df["file_etag"].isna().all()
    assert df["crs_id"].tolist() == ["2021000035", "2021000035c"]
    assert df["project_number"].tolist() == ["00123", "4567"]
    types = crdf._schema()["columns"]
    for column in mapped:
        assert str(df[column].dtype) == str(pd.Series(dtype=types[column]).dtype)


def test_raw_headers_kept_without_pre_process(workbook_for):
    fixture = LAYOUTS["provider_2026"]
    df = download_crdf(
        "provider", file=workbook_for("provider_2026"), pre_process=False
    )

    assert list(df.columns) == [
        *fixture["headers"],
        "price_year",
        "vintage",
        "source_file",
        "file_etag",
    ]
    assert df["price_year"].tolist() == [2024, 2024]


def test_year_filter_applies_to_a_two_year_file(workbook_for):
    df = download_crdf(
        "recipient", file=workbook_for("recipient_2026"), start_year=2015
    )

    assert df["year"].tolist() == [2015]


def test_numeric_ids_keep_their_digits(workbook_for):
    fixture = LAYOUTS["provider_2026"]
    position = fixture["headers"].index("CRS Identification N°")
    rows = [list(row) for row in fixture["rows"]]
    rows[1][position] = None
    path = workbook_for("provider_2026", rows=rows)

    df = download_crdf("provider", file=path)

    assert df["crs_id"].iloc[0] == "2021000035"
    assert pd.isna(df["crs_id"].iloc[1])


def _renamed(headers: list[str], old: str, new: str) -> list[str]:
    return [new if h == old else h for h in headers]


CURRENT = LAYOUTS["provider_2026"]["headers"]
CONSTANT_CLIMATE = (
    "Climate-related development finance - Commitment - 2024 USD thousand"
)


@pytest.mark.parametrize(
    "headers,unmapped,missing",
    [
        pytest.param(
            _renamed(CURRENT, "Pupose Code Name", "Purpose Code Name"),
            ("Purpose Code Name",),
            ("Pupose Code Name",),
            id="typo-fixed",
        ),
        pytest.param(CURRENT[:-1], (), ("Methodology",), id="column-dropped"),
        pytest.param([*CURRENT, "Notes"], ("Notes",), (), id="column-added"),
    ],
)
def test_unknown_header_row_raises(workbook_for, headers, unmapped, missing):
    rows = [[*row, None][: len(headers)] for row in LAYOUTS["provider_2026"]["rows"]]
    path = workbook_for("provider_2026", headers=headers, rows=rows)

    with pytest.raises(CRDFSchemaError) as info:
        download_crdf("provider", file=path)

    assert info.value.layout == "provider_2026"
    assert info.value.unmapped == unmapped
    assert info.value.missing == missing
    assert info.value.sheet == "2024"


def test_two_price_years_raise(workbook_for):
    headers = _renamed(
        CURRENT, CONSTANT_CLIMATE, CONSTANT_CLIMATE.replace("2024", "2023")
    )
    path = workbook_for("provider_2026", headers=headers)

    with pytest.raises(CRDFSchemaError) as info:
        download_crdf("provider", file=path)

    assert info.value.layout == "provider_2026"
    assert not info.value.unmapped
    assert not info.value.missing


def test_duplicated_header_raises(workbook_for):
    headers = _renamed(CURRENT, "Gender", "Biodiversity")
    path = workbook_for("provider_2026", headers=headers)

    with pytest.raises(CRDFSchemaError) as info:
        download_crdf("provider", file=path)

    assert info.value.layout is None


def test_other_perspective_raises(workbook_for):
    with pytest.raises(CRDFSchemaError) as info:
        download_crdf("provider", file=workbook_for("recipient_2026"))

    assert info.value.layout == "recipient_2026"


def test_text_in_a_numeric_column_raises(workbook_for):
    fixture = LAYOUTS["provider_2026"]
    position = fixture["headers"].index("Purpose Code")
    rows = [list(row) for row in fixture["rows"]]
    rows[1][position] = "not a code"
    path = workbook_for("provider_2026", rows=rows)

    with pytest.raises(CRDFSchemaError) as info:
        download_crdf("provider", file=path)

    assert info.value.sheet == "2024"


def test_listing_resolves_both_perspectives(live_folder):
    listing = list_crdf_files()

    by_name = listing.set_index("name")
    assert by_name.loc["CRDF-RP_2014-2015.xlsx", "perspective"] == "recipient"
    assert by_name.loc["CRDF-RP_2014-2015.xlsx", "start_year"] == 2014
    assert by_name.loc["CRDF-RP_2014-2015.xlsx", "end_year"] == 2015
    assert by_name.loc["CRDF-PP_2024.xlsx", "perspective"] == "provider"
    assert by_name.loc["CRDF-PP_2024.xlsx", "start_year"] == 2024
    assert by_name.loc["CRDF-PP_2024.xlsx", "end_year"] == 2024
    assert pd.isna(by_name.loc["Imputed_multilateral_shares.xlsx", "perspective"])
    assert pd.isna(by_name.loc["Imputed_multilateral_shares.xlsx", "start_year"])
    assert by_name.loc["CRDF-PP_all years-2013-2024.xlsx", "url"] == (
        "https://webfs.oecd.org/climate/ProviderPerspective/"
        "CRDF-PP_all%20years-2013-2024.xlsx"
    )
    assert by_name.loc["CRDF-PP_2024.xlsx", "size_bytes"] == 7652045


@pytest.mark.parametrize(
    "perspective,start,end,expected",
    [
        ("recipient", 2015, 2015, ["CRDF-RP_2014-2015.xlsx"]),
        ("recipient", 2014, 2016, ["CRDF-RP_2014-2015.xlsx", "CRDF-RP_2016.xlsx"]),
        ("provider", 2013, 2014, ["CRDF-PP_2013-2014.xlsx"]),
        ("provider", 2024, 2024, ["CRDF-PP_2024.xlsx"]),
    ],
)
def test_narrowest_file_holds_each_year(live_folder, perspective, start, end, expected):
    files, _, _ = crdf._files_for_years(list_crdf_files(), perspective, start, end)

    assert files["name"].tolist() == expected


def test_narrower_file_wins_over_a_wider_one_that_sorts_first():
    listing = pd.DataFrame(
        {
            "name": ["CRDF-RP_2000-2024.xlsx", "CRDF-RP_2015.xlsx"],
            "perspective": ["recipient", "recipient"],
            "start_year": pd.array([2000, 2015], dtype="Int32"),
            "end_year": pd.array([2024, 2015], dtype="Int32"),
        }
    )

    files, _, _ = crdf._files_for_years(listing, "recipient", 2015, 2015)

    assert files["name"].tolist() == ["CRDF-RP_2015.xlsx"]


def test_default_years_skip_the_all_years_file(live_folder):
    files, start, end = crdf._files_for_years(
        list_crdf_files(), "recipient", None, None
    )

    assert (start, end) == (2000, 2024)
    assert "CRDF-RP_all_years_2000-2024.xlsx" not in files["name"].tolist()


@pytest.mark.parametrize("start,end", [(1999, 2000), (2024, 2025), (2020, 2019)])
def test_years_outside_the_folder_raise(live_folder, start, end):
    with pytest.raises(ValueError):
        crdf._files_for_years(list_crdf_files(), "provider", start, end)


def test_download_reads_each_resolved_file(live_folder, monkeypatch, workbook_for):
    path = workbook_for("recipient_2026")
    fetched = []

    def fake_fetch(url):
        fetched.append(url)
        return path, '"etag-1"'

    monkeypatch.setattr(crdf, "_fetch", fake_fetch)

    df = download_crdf("recipient", start_year=2015, end_year=2015)

    assert fetched == [
        "https://webfs.oecd.org/climate/RecipientPerspective/CRDF-RP_2014-2015.xlsx"
    ]
    assert df["year"].tolist() == [2015]
    assert df["file_etag"].tolist() == ['"etag-1"']
    assert df["source_file"].tolist() == ["CRDF-RP_2014-2015.xlsx"]


def test_download_file_saves_into_a_directory(live_folder, monkeypatch, tmp_path):
    cached = tmp_path / "cached.xlsx"
    cached.write_bytes(b"workbook")
    monkeypatch.setattr(crdf, "_fetch", lambda url: (cached, None))
    target = tmp_path / "out"
    target.mkdir()

    saved = download_crdf_file("Imputed_multilateral_shares.xlsx", target)

    assert saved == target / "Imputed_multilateral_shares.xlsx"
    assert saved.read_bytes() == b"workbook"


def test_download_file_rejects_unknown_names(live_folder, tmp_path):
    with pytest.raises(ValueError):
        download_crdf_file("CRDF-PP_2024.xls", tmp_path)
