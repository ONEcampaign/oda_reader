"""Tests for `scan`, `release` and `cache_info` on recorded bulk zip fixtures."""

import hashlib
import shutil
import socket
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import oda_reader
from oda_reader import (
    BulkFileNotCachedError,
    cache_info,
    register_bulk_file,
    release,
    scan,
)
from oda_reader._cache.config import get_cache_dir
from oda_reader._cache.manager import bulk_cache_manager
from oda_reader.download.download_tools import _parse_ext_resource_entries
from oda_reader.schemas.bulk_columns import (
    MULTISYSTEM_COLUMNS,
    converted_columns,
)

_FIXTURES = Path(__file__).parents[1] / "fixtures"
_BASE = "https://webfs-dcd.oecd.org/files/dotStat"
_URLS = {
    "dac1": f"{_BASE}/DSD_DAC1/Table1_Data.zip",
    "dac2a": f"{_BASE}/DSD_DAC2/Table2a_Data.zip",
    "multisystem": f"{_BASE}/DSD_MULTI/MultiSystem%20entire%20dataset.zip",
    "crs": f"{_BASE}/DSD_CRS/CRS.parquet",
}
_ZIPS = {
    "dac1": _FIXTURES / "bulk" / "Table1_Data.zip",
    "dac2a": _FIXTURES / "bulk" / "Table2a_Data.zip",
    "multisystem": _FIXTURES / "bulk" / "MultiSystem entire dataset.zip",
}
_CRS_FRAME = pd.DataFrame({"donor_code": [1, 2], "usd_disbursement": [1.5, 2.5]})

# The converted columns for each dataset, in file order. oda-data builds its
# views on these names, so a change here is a breaking change.
_DOTSTAT_TYPES = {
    "donor_code": pa.int32(),
    "donor_name": pa.string(),
    "recipient_code": pa.int32(),
    "recipient_name": pa.string(),
    "part_code": pa.int32(),
    "part_name": pa.string(),
    "aidtype_code": pa.int32(),
    "aid_type": pa.string(),
    "flows_code": pa.int32(),
    "fund_flows": pa.string(),
    "amounttype_code": pa.string(),
    "data_type_code": pa.string(),
    "amount_type": pa.string(),
    "year": pa.int32(),
    "value": pa.float64(),
    "flags": pa.string(),
}
_PINNED_COLUMNS = {
    "dac1": (
        "donor_code",
        "donor_name",
        "part_code",
        "part_name",
        "aidtype_code",
        "aid_type",
        "flows_code",
        "fund_flows",
        "amounttype_code",
        "amount_type",
        "year",
        "value",
        "flags",
    ),
    "dac2a": (
        "recipient_code",
        "recipient_name",
        "donor_code",
        "donor_name",
        "part_code",
        "part_name",
        "aidtype_code",
        "aid_type",
        "data_type_code",
        "amount_type",
        "year",
        "value",
        "flags",
    ),
    "multisystem": (
        "year",
        "donor_code",
        "donor_name",
        "agency_name",
        "amount",
        "flow_type",
        "amount_type",
        "aid_to_or_thru",
        "channel_code",
        "channel_name",
        "channel_reported_name",
        "channel_category_code",
        "channel_category_name",
        "recipient_code",
        "recipient_name",
        "purpose_code",
        "purpose_name",
        "short_description",
        "gender",
        "environment",
        "ftc",
        "pba",
        "investment_project",
        "biodiversity",
        "climate_mitigation",
        "climate_adaptation",
        "desertification",
        "region_name",
        "incomegroup_name",
        "flow_code",
        "flow_name",
        "aid_t",
        "aid_t_name",
        "sector_code",
        "sector_name",
        "geography",
        "project_title",
        "long_description",
        "crs_id",
        "project_number",
    ),
}
# The first data row of each fixture, for the pinned Multisystem fields and
# the DAC tables' codes and amounts.
_FIRST_ROWS = {
    "dac1": {
        "donor_code": 20011,
        "donor_name": "DAC EU Members, Total",
        "part_code": 1,
        "aidtype_code": 12110,
        "flows_code": 1160,
        "amounttype_code": "A",
        "year": 2019,
        "value": 172.09,
        "flags": None,
    },
    "dac2a": {
        "recipient_code": 880,
        "recipient_name": "Samoa",
        "donor_code": 20001,
        "aidtype_code": 106,
        "data_type_code": "D",
        "year": 2014,
        "value": 30.531704,
    },
    "multisystem": {
        "year": 2011,
        "donor_code": 801,
        "donor_name": "Australia",
        "agency_name": "Australian Government",
        "channel_code": 40999,
        "recipient_code": 625,
        "purpose_code": 72040,
        "flow_type": "Commitments",
        "amount_type": "Constant prices",
        "aid_to_or_thru": "Contributions through",
        "amount": 5.382258,
        "short_description": None,
        "gender": 0,
        "ftc": None,
    },
}


class FakeHost:
    """Stands in for OECD: annotation lookup, version probe and file download."""

    def __init__(self, mocker, crs_parquet: Path) -> None:
        self.tokens = {dataset: f"etag-{dataset}-1" for dataset in _URLS}
        self.label_release = "v20260803"
        self.payloads = {**_ZIPS, "crs": crs_parquet}
        self.resolve = mocker.patch(
            "oda_reader._tables._resolve_bulk_file", side_effect=self._resolve
        )
        mocker.patch(
            "oda_reader._tables._fetch_revalidation_token", side_effect=self._token
        )
        self.download = mocker.patch(
            "oda_reader.download.download_tools._stream_to_file",
            side_effect=self._download,
        )

    def _dataset(self, url: str) -> str:
        return next(name for name, known in _URLS.items() if known == url)

    def _resolve(self, flow_url, label, latest_flow=None, **_kwargs):
        dataset = next(
            name
            for name, annotation in oda_reader.download.download_tools.BULK_DATASETS.items()
            if annotation == (flow_url, label)
        )
        return _URLS[dataset], (self.label_release if dataset == "crs" else None)

    def _token(self, url):
        return self.tokens[self._dataset(url)]

    def _download(self, url, headers, path):
        shutil.copyfile(self.payloads[self._dataset(url)], path)


@pytest.fixture
def crs_parquet(tmp_path):
    path = tmp_path / "CRS.parquet"
    _CRS_FRAME.to_parquet(path)
    return path


@pytest.fixture
def host(mocker, crs_parquet):
    return FakeHost(mocker, crs_parquet)


@pytest.fixture
def no_sockets(mocker):
    refuse = OSError("sockets blocked in test")
    return {
        "getaddrinfo": mocker.patch("socket.getaddrinfo", side_effect=refuse),
        "connect": mocker.patch.object(socket.socket, "connect", side_effect=refuse),
    }


def _assert_no_network(no_sockets):
    no_sockets["getaddrinfo"].assert_not_called()
    no_sockets["connect"].assert_not_called()


@pytest.mark.unit
class TestConvertedColumns:
    @pytest.mark.parametrize("dataset", ["dac1", "dac2a", "multisystem"])
    def test_columns_are_pinned(self, host, dataset):
        table = scan(dataset)

        assert table.columns == _PINNED_COLUMNS[dataset]
        assert tuple(pq.read_schema(table.path).names) == table.columns
        folded = [name.casefold() for name in table.columns]
        assert len(set(folded)) == len(folded)

    @pytest.mark.parametrize("dataset", ["dac1", "dac2a"])
    def test_dotstat_types(self, host, dataset):
        schema = pq.read_schema(scan(dataset).path)

        assert {f.name: f.type for f in schema} == {
            name: _DOTSTAT_TYPES[name] for name in _PINNED_COLUMNS[dataset]
        }

    def test_multisystem_types(self, host):
        schema = pq.read_schema(scan("multisystem").path)

        expected = {
            column.name: column.type
            for column in MULTISYSTEM_COLUMNS.values()
            if column is not None
        }
        assert {f.name: f.type for f in schema} == expected

    @pytest.mark.parametrize("dataset", ["dac1", "dac2a", "multisystem"])
    def test_values_survive_conversion(self, host, dataset):
        table = pq.read_table(scan(dataset).path)

        assert table.num_rows == 5
        first = table.slice(0, 1).to_pylist()[0]
        expected = _FIRST_ROWS[dataset]
        assert {name: first[name] for name in expected} == pytest.approx(expected)

    def test_unknown_header_becomes_a_snake_case_string(self):
        columns = converted_columns(
            ["DonorCode", "NewMarker Flag"], MULTISYSTEM_COLUMNS
        )

        assert [c.name for c in columns.values()] == ["donor_code", "new_marker_flag"]
        assert columns["NewMarker Flag"].type == pa.string()

    def test_case_insensitive_collision_raises(self):
        with pytest.raises(ValueError, match="both convert to"):
            converted_columns(["DonorCode", "Donor Code"], MULTISYSTEM_COLUMNS)

    def test_row_groups_hold_a_fixed_row_count(self, host, mocker):
        mocker.patch("oda_reader._tables._ROW_GROUP_ROWS", 2)

        metadata = pq.ParquetFile(scan("dac1").path).metadata

        sizes = [metadata.row_group(i).num_rows for i in range(metadata.num_row_groups)]
        assert sizes == [2, 2, 1]
        assert metadata.row_group(0).column(0).statistics.has_min_max


@pytest.mark.unit
class TestScan:
    def test_crs_is_a_hard_link_to_the_cached_file(self, host):
        table = scan("crs")

        [info] = cache_info()
        assert table.path != info.path
        assert table.path.samefile(info.path)
        assert table.release == "etag-crs-1"
        assert table.label_release == "v20260803"
        assert table.columns == ("donor_code", "usd_disbursement")
        pd.testing.assert_frame_equal(pd.read_parquet(table.path), _CRS_FRAME)

    def test_crs_downloaded_earlier_gains_its_label_release_on_a_matching_token(
        self, host, crs_parquet
    ):
        register_bulk_file(
            crs_parquet, dataset="crs", url=_URLS["crs"], version="etag-crs-1"
        )

        table = scan("crs")

        assert table.release == "etag-crs-1"
        assert table.label_release == "v20260803"
        host.download.assert_not_called()

    def test_crs_refetched_under_the_same_label_gets_a_new_token(self, host):
        first = scan("crs")

        host.tokens["crs"] = "etag-crs-2"
        second = scan("crs")

        assert host.download.call_count == 2
        assert second.label_release == first.label_release == "v20260803"
        assert second.release == "etag-crs-2"
        assert second.path != first.path

    def test_same_token_reuses_the_conversion(self, host, mocker):
        spy = mocker.spy(oda_reader._tables, "_write_parquet")

        first = scan("dac1")
        mtime = first.path.stat().st_mtime_ns
        second = scan("dac1")

        assert second == first
        assert second.release == "etag-dac1-1"
        assert second.path.stat().st_mtime_ns == mtime
        assert spy.call_count == 1
        assert host.download.call_count == 1

    def test_new_token_downloads_and_reconverts(self, host, mocker):
        spy = mocker.spy(oda_reader._tables, "_write_parquet")
        first = scan("dac2a")

        host.tokens["dac2a"] = "etag-dac2a-2"
        second = scan("dac2a")

        assert second.release == "etag-dac2a-2"
        assert second.path != first.path
        assert spy.call_count == 2
        assert host.download.call_count == 2

    @pytest.mark.parametrize("dataset", ["dac1", "crs"])
    def test_a_returned_path_keeps_its_bytes_after_a_new_release(
        self, host, tmp_path, dataset
    ):
        first = scan(dataset)
        before = pq.read_table(first.path)

        new_crs = tmp_path / "CRS-2.parquet"
        _CRS_FRAME.assign(usd_disbursement=[9.0, 9.0]).to_parquet(new_crs)
        host.payloads = {**host.payloads, "crs": new_crs, "dac1": _ZIPS["dac2a"]}
        host.tokens[dataset] = f"etag-{dataset}-2"
        second = scan(dataset)

        assert not pq.read_table(second.path).equals(before)
        assert pq.read_table(first.path).equals(before)

    @pytest.mark.parametrize("dataset", ["dac2a", "crs"])
    def test_files_before_the_previous_release_are_removed(self, host, dataset):
        paths = []
        for n in (1, 2, 3):
            host.tokens[dataset] = f"etag-{dataset}-{n}"
            paths.append(scan(dataset).path)

        assert [path.exists() for path in paths] == [False, True, True]

    def test_failed_conversion_leaves_no_file(self, host, mocker):
        def write_then_fail(z, dest, columns):
            dest.write_bytes(b"PAR1 half-written")
            raise OSError("disk full")

        mocker.patch("oda_reader._tables._write_parquet", side_effect=write_then_fail)
        with pytest.raises(OSError, match="disk full"):
            scan("dac1")

        bulk_dir = bulk_cache_manager().base_dir
        assert list(bulk_dir.glob("*.parquet")) == []
        assert list(bulk_dir.glob("*.tmp-*")) == []

        mocker.stopall()
        FakeHost(mocker, host.payloads["crs"])
        assert pq.read_table(scan("dac1").path).num_rows == 5

    @pytest.mark.parametrize(
        ("dataset", "columns"), [("dac1", MULTISYSTEM_COLUMNS), ("crs", None)]
    )
    def test_refuses_a_payload_replaced_after_its_record_was_read(
        self, host, dataset, columns
    ):
        scan(dataset)
        manager = bulk_cache_manager()
        key = cache_info()[0].path.stem
        found = manager.find_record(key=key)
        assert found is not None
        stale = {**found[2], "seq": found[2]["seq"] - 1}

        with pytest.raises(RuntimeError, match="replaced"):
            oda_reader._tables._table_file(
                manager, key, stale, "another-token", columns
            )

    def test_conversion_cleared_midway_leaves_no_file(self, host, mocker):
        write = oda_reader._tables._write_parquet

        def clear_then_write(z, dest, columns):
            bulk_cache_manager().clear()
            write(z, dest, columns)

        mocker.patch("oda_reader._tables._write_parquet", side_effect=clear_then_write)
        with pytest.raises(RuntimeError, match="removed"):
            scan("dac1")

        assert list(bulk_cache_manager().base_dir.glob("*.parquet")) == []

    def test_cache_dir_scopes_the_call(self, host, tmp_path):
        root = get_cache_dir()
        scoped = tmp_path / "scoped"

        table = scan("multisystem", cache_dir=scoped)

        assert table.path.is_relative_to(scoped)
        assert get_cache_dir() == root
        assert cache_info() == []
        assert [info.dataset for info in cache_info(cache_dir=scoped)] == [
            "multisystem"
        ]
        assert release("multisystem", cache_dir=scoped, offline=True) == table.release

    def test_offline_cold_cache_raises(self, no_sockets):
        with pytest.raises(BulkFileNotCachedError) as exc_info:
            scan("dac2a", offline=True)

        assert exc_info.value.dataset == "dac2a"
        _assert_no_network(no_sockets)

    def test_offline_reads_a_registered_file_without_network(self, no_sockets):
        register_bulk_file(
            _ZIPS["multisystem"],
            dataset="multisystem",
            url=_URLS["multisystem"],
            version="etag-registered",
        )

        table = scan("multisystem", offline=True)

        assert table.release == "etag-registered"
        assert pq.read_table(table.path).num_rows == 5
        _assert_no_network(no_sockets)

    def test_clearing_the_bulk_file_removes_its_conversion(self, host):
        table = scan("dac1")

        bulk_cache_manager().clear()

        assert not table.path.exists()

    @pytest.mark.parametrize(
        ("dataset", "kwargs"),
        [("dac2b", {}), ("crs", {"refresh": True, "offline": True})],
    )
    def test_bad_arguments_raise(self, dataset, kwargs):
        with pytest.raises(ValueError):
            scan(dataset, **kwargs)


@pytest.mark.unit
class TestRelease:
    def test_online_crs_is_the_host_token(self, host):
        assert release("crs") == "etag-crs-1"
        host.download.assert_not_called()

    @pytest.mark.parametrize("version", [None, "etag-registered"])
    def test_registered_crs_agrees_with_scan_when_the_probe_fails(
        self, host, crs_parquet, version
    ):
        register_bulk_file(
            crs_parquet, dataset="crs", url=_URLS["crs"], version=version
        )
        host.tokens["crs"] = None

        table = scan("crs")

        assert release("crs") == table.release
        assert release("crs", offline=True) == table.release
        assert table.label_release is None
        host.download.assert_not_called()

    def test_online_dac1_is_the_host_token(self, host):
        assert release("dac1") == "etag-dac1-1"
        host.download.assert_not_called()

    def test_online_without_a_host_token_uses_the_cached_copy(self, host):
        scan("dac1")
        host.tokens["dac1"] = None

        assert release("dac1") == "etag-dac1-1"

    def test_online_without_a_host_token_or_a_copy_raises(self, host):
        host.tokens["dac1"] = None

        with pytest.raises(BulkFileNotCachedError):
            release("dac1")

    def test_file_without_a_token_is_hashed_once(self, mocker):
        mocker.patch(
            "oda_reader.download.download_tools._fetch_revalidation_token",
            return_value=None,
        )
        register_bulk_file(_ZIPS["dac1"], dataset="dac1", url=_URLS["dac1"])
        digest = hashlib.sha256(_ZIPS["dac1"].read_bytes()).hexdigest()

        assert release("dac1", offline=True) == f"sha256:{digest}"
        assert scan("dac1", offline=True).release == f"sha256:{digest}"
        assert cache_info()[0].release == f"sha256:{digest}"

    def test_offline_cold_cache_raises(self, no_sockets):
        with pytest.raises(BulkFileNotCachedError):
            release("crs", offline=True)
        _assert_no_network(no_sockets)


@pytest.mark.unit
@pytest.mark.parametrize("dataset", ["dac2a", "crs"])
def test_cache_info_lists_bulk_files(host, dataset):
    table = scan(dataset)

    [info] = cache_info()

    assert info.dataset == dataset
    assert info.release == table.release
    assert info.label_release == table.label_release
    assert info.downloaded == table.downloaded
    assert info.size_bytes == host.payloads[dataset].stat().st_size
    # A CRS hard link shares the cached file's disk space.
    assert info.derived_bytes == (0 if dataset == "crs" else table.path.stat().st_size)
    assert info.stale is False


@pytest.mark.unit
def test_annotation_labels_keep_their_release_id():
    xml = (_FIXTURES / "dataflow" / "dataflow_crs.xml").read_text()
    dac1_xml = (_FIXTURES / "dataflow" / "dataflow_dac1.xml").read_text()

    entries = _parse_ext_resource_entries(xml)

    assert entries["CRS-Parquet"] == (_URLS["crs"], "v20260803")
    assert (
        _parse_ext_resource_entries(dac1_xml)["DAC1 full dataset (dotStat format)"][1]
        is None
    )
