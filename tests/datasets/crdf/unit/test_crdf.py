"""Unit tests for the CRDF reader.

Workbooks are built in tmp_path from tests/fixtures/crdf/layouts.json, which
holds the real header row of each known layout and two synthetic data rows.
The folder listings are snapshots of https://webfs.oecd.org/climate/.
"""

import json
import os
import shutil
from contextlib import nullcontext
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


def _host_serving(workbook: Path, validator: tuple[str, str] | None):
    """A `_stream_to_file` fake that writes *workbook* and delivers *validator*."""

    def stream(url, headers, target):
        shutil.copyfile(workbook, target)
        return validator

    return stream


@pytest.fixture
def live_folder(monkeypatch):
    """Serve the listing snapshots and let _fetch return fixture workbooks."""

    def fake_get(url):
        return 200, (FIXTURES / LISTINGS[url]).read_text(encoding="utf-8")

    monkeypatch.setattr(crdf, "_get_listing", fake_get)


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
        "file_version",
        "file_version_kind",
    }
    assert len(df) == 2
    assert df["price_year"].tolist() == [fixture["price_year"]] * 2
    assert (
        df["vintage"].tolist()
        == [datetime.fromisoformat(fixture["modified"]).date()] * 2
    )
    assert df["source_file"].tolist() == [fixture["file"]] * 2
    assert df[["file_version", "file_version_kind"]].isna().all().all()
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
        "file_version",
        "file_version_kind",
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
    files = crdf._files_for_years(list_crdf_files(), perspective, start, end)

    assert files["name"].tolist() == expected
    assert sorted(y for years in files["years"] for y in years) == list(
        range(start, end + 1)
    )


def test_narrower_file_wins_over_a_wider_one_that_sorts_first():
    listing = pd.DataFrame(
        {
            "name": ["CRDF-RP_2000-2024.xlsx", "CRDF-RP_2015.xlsx"],
            "perspective": ["recipient", "recipient"],
            "start_year": pd.array([2000, 2015], dtype="Int32"),
            "end_year": pd.array([2024, 2015], dtype="Int32"),
        }
    )

    files = crdf._files_for_years(listing, "recipient", 2015, 2015)

    assert files["name"].tolist() == ["CRDF-RP_2015.xlsx"]


def test_default_years_skip_the_all_years_file(live_folder):
    files = crdf._files_for_years(list_crdf_files(), "recipient", None, None)

    assert sorted(y for years in files["years"] for y in years) == list(
        range(2000, 2025)
    )
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
        return nullcontext((path, '"etag-1"', "etag"))

    monkeypatch.setattr(crdf, "_fetch", fake_fetch)

    df = download_crdf("recipient", start_year=2015, end_year=2015)

    assert fetched == [
        "https://webfs.oecd.org/climate/RecipientPerspective/CRDF-RP_2014-2015.xlsx"
    ]
    assert df["year"].tolist() == [2015]
    assert df["file_version"].tolist() == ['"etag-1"']
    assert df["file_version_kind"].tolist() == ["etag"]
    assert df["source_file"].tolist() == ["CRDF-RP_2014-2015.xlsx"]


def test_overlapping_workbooks_each_contribute_only_their_years(
    monkeypatch, workbook_for
):
    listing = pd.DataFrame(
        {
            "name": ["CRDF-RP_2014-2015.xlsx", "CRDF-RP_2015.xlsx"],
            "perspective": ["recipient", "recipient"],
            "start_year": pd.array([2014, 2015], dtype="Int32"),
            "end_year": pd.array([2015, 2015], dtype="Int32"),
            "url": ["https://webfs.oecd.org/a.xlsx", "https://webfs.oecd.org/b.xlsx"],
        }
    )
    path = workbook_for("recipient_2026")  # one row each for 2014 and 2015
    monkeypatch.setattr(crdf, "list_crdf_files", lambda: listing)
    monkeypatch.setattr(crdf, "_fetch", lambda url: nullcontext((path, None, None)))

    df = download_crdf("recipient", start_year=2014, end_year=2015)

    assert df["year"].tolist() == [2014, 2015]
    assert df["source_file"].tolist() == [
        "CRDF-RP_2014-2015.xlsx",
        "CRDF-RP_2015.xlsx",
    ]


def test_empty_folder_listing_raises_schema_error(monkeypatch):
    monkeypatch.setattr(crdf, "_get_listing", lambda url: (200, "<html>moved</html>"))

    with pytest.raises(CRDFSchemaError) as info:
        list_crdf_files()

    assert info.value.source == crdf.CRDF_URL


def test_listing_request_has_a_timeout(monkeypatch):
    seen = {}

    def fake_get_same_origin(getter, url, headers, **kwargs):
        seen.update(kwargs)
        raise RuntimeError("stop")

    monkeypatch.setattr(crdf, "_get_same_origin", fake_get_same_origin)

    with pytest.raises(RuntimeError):
        crdf._get_listing(crdf.CRDF_URL)

    assert seen["timeout"] == (10, 30)


def test_download_file_saves_into_a_directory(live_folder, monkeypatch, tmp_path):
    cached = tmp_path / "cached.xlsx"
    cached.write_bytes(b"workbook")
    monkeypatch.setattr(crdf, "_fetch", lambda url: nullcontext((cached, None, None)))
    target = tmp_path / "out"
    target.mkdir()

    saved = download_crdf_file("Imputed_multilateral_shares.xlsx", target)

    assert saved == target / "Imputed_multilateral_shares.xlsx"
    assert saved.read_bytes() == b"workbook"


def test_download_file_rejects_unknown_names(live_folder, tmp_path):
    with pytest.raises(ValueError):
        download_crdf_file("CRDF-PP_2024.xls", tmp_path)


def test_cache_hit_reports_the_version_recorded_with_the_copy(
    monkeypatch, workbook_for
):
    """A failed probe still serves the cached copy and reports its validator."""
    workbook = workbook_for("recipient_2026")
    url = crdf.CRDF_URL + "RecipientPerspective/CRDF-RP_2016.xlsx"
    validator = ("Tue, 07 Jul 2026 13:05:26 GMT", "last_modified")
    validators = iter([validator, None])
    monkeypatch.setattr(
        crdf, "_fetch_revalidation_validator", lambda u: next(validators)
    )
    monkeypatch.setattr(crdf, "_stream_to_file", _host_serving(workbook, validator))

    with crdf._fetch(url) as (_, *downloaded):
        pass
    with crdf._fetch(url) as (_, *served):
        pass

    assert served == downloaded == ["Tue, 07 Jul 2026 13:05:26 GMT", "last_modified"]


@pytest.mark.parametrize(
    "name", ["../outside.xlsx", "..", ".", "sub/inner.xlsx", "sub\\inner.xlsx"]
)
def test_listing_entry_that_is_not_a_plain_name_raises(monkeypatch, name):
    listing = (
        f'<pre>  6/4/2026  6:35 PM        96641 <A HREF="/climate/x.xlsx">{name}</A>'
        "<br></pre>"
    )
    monkeypatch.setattr(crdf, "_get_listing", lambda url: (200, listing))

    with pytest.raises(CRDFSchemaError) as info:
        list_crdf_files()

    assert repr(name) in info.value.detail


def test_download_file_refuses_a_destination_outside_the_directory(
    monkeypatch, tmp_path
):
    listing = pd.DataFrame(
        {"name": ["../outside.xlsx"], "url": ["https://webfs.oecd.org/x.xlsx"]}
    )
    monkeypatch.setattr(crdf, "list_crdf_files", lambda: listing)
    monkeypatch.setattr(
        crdf, "_fetch", lambda url: pytest.fail("fetched before the path check")
    )
    target = tmp_path / "out"
    target.mkdir()

    with pytest.raises(ValueError, match="outside"):
        download_crdf_file("../outside.xlsx", target)

    assert not (tmp_path / "outside.xlsx").exists()


def _refuse_link(src, dst):
    raise OSError("hard links not supported")


@pytest.mark.parametrize("hard_links", [True, False], ids=["linked", "copied"])
def test_rows_carry_the_version_of_the_bytes_they_came_from(
    live_folder, monkeypatch, tmp_path, hard_links
):
    """Another process replacing the cached workbook mid-read changes no label."""
    fixture = LAYOUTS["recipient_2026"]
    v1 = write_workbook(tmp_path / "v1.xlsx", fixture)
    v2 = write_workbook(
        tmp_path / "v2.xlsx", {**fixture, "modified": "2027-01-01T00:00:00"}
    )
    url = crdf.CRDF_URL + "RecipientPerspective/CRDF-RP_2014-2015.xlsx"
    monkeypatch.setattr(
        crdf, "_fetch_revalidation_validator", lambda u: ('"v1"', "etag")
    )
    monkeypatch.setattr(crdf, "_stream_to_file", _host_serving(v1, ('"v1"', "etag")))
    if not hard_links:
        monkeypatch.setattr(os, "link", _refuse_link)

    def replace_cached_workbook() -> None:
        key = crdf._bulk_cache_key(url)
        crdf.bulk_cache_manager().ensure(
            crdf.CacheEntry(
                key=key,
                filename=f"{key}.xlsx",
                fetcher=lambda target: _host_serving(v2, ('"v2"', "etag"))(
                    url, {}, target
                ),
                version='"v2"',
                version_kind="etag",
                url=url,
            )
        )

    # The read starts after the cache lock is released: the point at which
    # another process can replace the cached file.
    read_headers = crdf._read_headers

    def replace_then_read_headers(path):
        replace_cached_workbook()
        return read_headers(path)

    monkeypatch.setattr(crdf, "_read_headers", replace_then_read_headers)

    df = download_crdf("recipient", start_year=2014, end_year=2015)

    versions = {
        datetime.fromisoformat(fixture["modified"]).date(): '"v1"',
        datetime(2027, 1, 1).date(): '"v2"',
    }
    assert len(df) == 2
    assert [versions[v] for v in df["vintage"]] == df["file_version"].tolist()
    [record] = crdf.bulk_cache_manager().list_records()
    assert record["version"] == '"v2"'
    assert not list(crdf.bulk_cache_manager().base_dir.glob(".snapshot-*"))
