"""Tests for `register_bulk_file`, offline bulk reads and registered entries."""

import hashlib
import socket

import pandas as pd
import pytest
import requests

from oda_reader import (
    BulkFileNotCachedError,
    bulk_download_crs,
    register_bulk_file,
)
from oda_reader._cache.manager import CacheEntry, CacheManager, bulk_cache_manager
from oda_reader.crs import CRS_BULK_LABEL, CRS_REDUCED_BULK_LABEL
from oda_reader.dac1 import DAC1_BULK_LABEL
from oda_reader.dac2a import DAC2A_BULK_LABEL
from oda_reader.dac2b import DAC2B_BULK_LABEL
from oda_reader.download.download_tools import (
    BULK_DATASETS,
    CRS_FLOW_URL,
    DAC1_FLOW_URL,
    DAC2A_FLOW_URL,
    DAC2B_FLOW_URL,
    MULTI_FLOW_URL,
    bulk_download_parquet,
)
from oda_reader.exceptions import (
    BulkDownloadChallengeError,
    BulkDownloadHTTPError,
    BulkPayloadCorruptError,
)
from oda_reader.multisystem import MULTISYSTEM_BULK_LABEL

freeze_time = pytest.importorskip("freezegun").freeze_time

_CRS_URL = "https://webfs-dcd.oecd.org/files/dotStat/DSD_CRS/CRS.parquet"
_CRS_KEY = hashlib.sha1(_CRS_URL.encode()).hexdigest()
_FRAME = pd.DataFrame({"donor_code": [1, 2, 3], "usd_disbursement": [1.5, 2.5, 3.5]})


@pytest.fixture
def crs_parquet(tmp_path):
    path = tmp_path / "downloads" / "CRS.parquet"
    path.parent.mkdir()
    _FRAME.to_parquet(path)
    return path


@pytest.fixture
def no_sockets(mocker):
    """Refuse every DNS lookup and socket connection, and record attempts."""
    refuse = OSError("sockets blocked in test")
    return {
        "getaddrinfo": mocker.patch("socket.getaddrinfo", side_effect=refuse),
        "connect": mocker.patch.object(socket.socket, "connect", side_effect=refuse),
    }


@pytest.fixture
def stream(mocker):
    """The bulk-file fetcher, patched to raise so no test downloads."""
    return mocker.patch(
        "oda_reader.download.download_tools._stream_to_file",
        side_effect=AssertionError("bulk file fetch attempted"),
    )


def _assert_no_network(no_sockets):
    no_sockets["getaddrinfo"].assert_not_called()
    no_sockets["connect"].assert_not_called()


@pytest.mark.unit
def test_socket_block_stops_a_real_request(no_sockets):
    """Control: the fixture the offline tests rely on does block requests."""
    with pytest.raises(requests.exceptions.ConnectionError):
        requests.get("https://sdmx.oecd.org/public/rest/dataflow", timeout=5)

    no_sockets["getaddrinfo"].assert_called()


@pytest.mark.unit
class TestOfflineRead:
    @pytest.mark.parametrize(
        "url",
        [
            _CRS_URL,
            # OECD renamed the file after it was registered; offline reads go by
            # dataset, so the stored URL's name does not matter.
            _CRS_URL.replace("CRS.parquet", "CRS_20261021T0900.parquet"),
        ],
    )
    def test_reads_registered_crs_with_sockets_blocked(
        self, crs_parquet, no_sockets, stream, url
    ):
        register_bulk_file(crs_parquet, dataset="crs", url=url, version="e1")

        result = bulk_download_crs(offline=True)

        pd.testing.assert_frame_equal(result, _FRAME)
        _assert_no_network(no_sockets)
        stream.assert_not_called()

    def test_nothing_cached_raises_named_error(self, no_sockets):
        with pytest.raises(BulkFileNotCachedError) as exc_info:
            bulk_download_crs(offline=True)

        assert exc_info.value.dataset == "crs"
        assert "register_bulk_file(" in str(exc_info.value)
        _assert_no_network(no_sockets)

    def test_registration_older_than_ttl_still_reads(
        self, crs_parquet, no_sockets, stream
    ):
        with freeze_time("2026-01-01"):
            register_bulk_file(crs_parquet, dataset="crs", url=_CRS_URL, version="e1")

        with freeze_time("2027-01-01"):
            result = bulk_download_crs(offline=True)

        pd.testing.assert_frame_equal(result, _FRAME)
        _assert_no_network(no_sockets)

    def test_download_older_than_ttl_still_reads(self, no_sockets, mocker):
        mocker.patch(
            "oda_reader.download.download_tools._stream_to_file",
            side_effect=lambda url, headers, path: _FRAME.to_parquet(path),
        )
        with freeze_time("2026-01-01"):
            bulk_download_parquet(
                url=_CRS_URL, flow_url=CRS_FLOW_URL, label=CRS_BULK_LABEL
            )

        with freeze_time("2026-06-01"):
            result = bulk_download_crs(offline=True)

        pd.testing.assert_frame_equal(result, _FRAME)
        _assert_no_network(no_sockets)

    def test_url_reads_entry_by_key(self, crs_parquet, no_sockets):
        register_bulk_file(crs_parquet, url=_CRS_URL, version="e1")

        result = bulk_download_parquet(url=_CRS_URL, offline=True)

        pd.testing.assert_frame_equal(result, _FRAME)
        _assert_no_network(no_sockets)


@pytest.mark.unit
class TestOnlineReadOfRegisteredEntry:
    """Online reads resolve and probe as usual; the token decides the hit."""

    @pytest.fixture(autouse=True)
    def _resolution(self, mocker):
        mocker.patch(
            "oda_reader.download.download_tools._resolve_bulk_file_url",
            return_value=_CRS_URL,
        )

    @pytest.mark.parametrize(
        "probed", ["e1", None], ids=["token-matches", "probe-failed"]
    )
    def test_serves_registered_file(
        self, crs_parquet, no_sockets, stream, mocker, probed
    ):
        register_bulk_file(crs_parquet, dataset="crs", url=_CRS_URL, version="e1")
        probe = mocker.patch(
            "oda_reader.download.download_tools._fetch_revalidation_token",
            return_value=probed,
        )

        with freeze_time("2027-01-01"):
            result = bulk_download_crs()

        pd.testing.assert_frame_equal(result, _FRAME)
        probe.assert_called_once_with(_CRS_URL)
        stream.assert_not_called()

    def test_new_token_downloads_the_republished_file(
        self, crs_parquet, no_sockets, mocker
    ):
        register_bulk_file(crs_parquet, dataset="crs", url=_CRS_URL, version="e1")
        mocker.patch(
            "oda_reader.download.download_tools._fetch_revalidation_token",
            return_value="e2",
        )
        republished = pd.DataFrame({"donor_code": [9]})
        mocker.patch(
            "oda_reader.download.download_tools._stream_to_file",
            side_effect=lambda url, headers, path: republished.to_parquet(path),
        )

        result = bulk_download_crs()

        pd.testing.assert_frame_equal(result, republished)
        [record] = bulk_cache_manager().list_records()
        assert (record["source"], record["version"]) == ("download", "e2")


@pytest.mark.unit
class TestRegisterBulkFile:
    def test_record_is_keyed_by_url_hash(self, crs_parquet, no_sockets):
        cached = register_bulk_file(
            crs_parquet, dataset="crs", url=_CRS_URL, version="e1"
        )

        assert cached.name == f"{_CRS_KEY}.parquet"
        [record] = bulk_cache_manager().list_records()
        assert record["key"] == _CRS_KEY
        assert record["source"] == "registered"
        assert record["is_stale"] is False

    def test_version_defaults_to_probed_token(self, crs_parquet, mocker):
        mocker.patch(
            "oda_reader.download.download_tools._fetch_revalidation_token",
            return_value='"etag-9"',
        )

        register_bulk_file(crs_parquet, dataset="crs", url=_CRS_URL)

        [record] = bulk_cache_manager().list_records()
        assert record["version"] == '"etag-9"'

    def test_dataset_without_url_resolves_annotation(self, crs_parquet, mocker):
        resolve = mocker.patch(
            "oda_reader.download.download_tools._resolve_bulk_file_url",
            return_value=_CRS_URL,
        )

        register_bulk_file(crs_parquet, dataset="crs", version="e1")

        resolve.assert_called_once_with(CRS_FLOW_URL, CRS_BULK_LABEL)
        [record] = bulk_cache_manager().list_records()
        assert record["url"] == _CRS_URL

    def test_corrupt_file_is_rejected_and_left_in_place(self, tmp_path):
        source = tmp_path / "CRS.parquet"
        source.write_bytes(b"<html>challenge page</html>")

        with pytest.raises(BulkPayloadCorruptError):
            register_bulk_file(source, dataset="crs", url=_CRS_URL, version="e1")

        assert source.read_bytes() == b"<html>challenge page</html>"
        assert bulk_cache_manager().list_records() == []
        assert list(bulk_cache_manager().base_dir.glob("*.tmp-*")) == []

    @pytest.mark.parametrize(
        "kwargs",
        [
            {},
            {"dataset": "crs-2022"},
            {"url": "https://example.com/CRS.parquet"},
        ],
        ids=["neither", "unknown-dataset", "off-allowlist-url"],
    )
    def test_invalid_arguments_raise(self, crs_parquet, kwargs):
        with pytest.raises(ValueError):
            register_bulk_file(crs_parquet, **kwargs)


@pytest.mark.unit
class TestRegisteredEntryLifetime:
    @staticmethod
    def _download(manager, key):
        manager.ensure(
            CacheEntry(key=key, filename=f"{key}.parquet", fetcher=_FRAME.to_parquet)
        )

    def test_expiry_sweep_keeps_registered_entry(self, tmp_path, crs_parquet):
        base = tmp_path / "bulk"
        manager = CacheManager(base_dir=base)
        with freeze_time("2026-01-01"):
            manager.register(
                crs_parquet, key="registered", filename="r.parquet", url=_CRS_URL
            )
            self._download(manager, "expired")
        with freeze_time("2026-06-01"):
            self._download(manager, "fresh")
            CacheManager(base_dir=base)  # runs the startup expiry sweep

            keys = {r["key"] for r in manager.list_records()}

        assert keys == {"registered", "fresh"}
        assert (base / "r.parquet").exists()

    def test_registration_is_not_refetched_after_ttl(self, tmp_path, crs_parquet):
        manager = CacheManager(base_dir=tmp_path / "bulk")
        with freeze_time("2026-01-01"):
            manager.register(crs_parquet, key="k", filename="k.parquet", url=_CRS_URL)

        with freeze_time("2027-01-01"):
            self._download(manager, "k")  # a stale record would refetch here
            [record] = manager.list_records()

        assert (record["source"], record["is_stale"]) == ("registered", False)


@pytest.mark.unit
class TestChallengeNamesRegistration:
    def test_reader_challenge_prints_full_register_call(self, mocker):
        mocker.patch(
            "oda_reader.download.download_tools._stream_to_file",
            side_effect=BulkDownloadChallengeError(
                status_code=403, url=_CRS_URL, body="", cf_ray="ray-1"
            ),
        )

        with pytest.raises(BulkDownloadChallengeError) as exc_info:
            bulk_download_parquet(
                url=_CRS_URL,
                flow_url=CRS_FLOW_URL,
                label=CRS_BULK_LABEL,
                version='"etag-1"',
            )

        error = exc_info.value
        assert (error.dataset, error.version, error.filename) == (
            "crs",
            '"etag-1"',
            "CRS.parquet",
        )
        assert (
            "register_bulk_file('~/Downloads/CRS.parquet', dataset='crs', "
            f"url='{_CRS_URL}', version='\"etag-1\"')"
        ) in str(error)

    def test_challenge_after_re_resolve_names_dataset_and_new_version(self, mocker):
        new_url = "https://webfs-dcd.oecd.org/files/dotStat/DSD_CRS/CRS-v2.parquet"
        mocker.patch(
            "oda_reader.download.download_tools._stream_to_file",
            side_effect=[
                BulkDownloadHTTPError(status_code=404, url=_CRS_URL, body=""),
                BulkDownloadChallengeError(
                    status_code=403, url=new_url, body="", cf_ray=None
                ),
            ],
        )
        mocker.patch(
            "oda_reader.download.download_tools.get_bulk_file_url_with_version",
            return_value=(new_url, '"etag-2"'),
        )

        with pytest.raises(BulkDownloadChallengeError) as exc_info:
            bulk_download_parquet(
                url=_CRS_URL,
                flow_url=CRS_FLOW_URL,
                label=CRS_BULK_LABEL,
                version='"etag-1"',
            )

        error = exc_info.value
        assert (error.dataset, error.version) == ("crs", '"etag-2"')
        assert "dataset='crs'" in str(error)

    def test_plain_url_challenge_prints_url_only_call(self):
        error = BulkDownloadChallengeError(
            status_code=403,
            url="https://webfs-dcd.oecd.org/files/CRS%202022%20data.zip",
            body="",
            cf_ray=None,
        )

        assert error.dataset is None
        assert error.filename == "CRS 2022 data.zip"
        assert "register_bulk_file('~/Downloads/CRS 2022 data.zip', url=" in str(error)


@pytest.mark.unit
def test_bulk_datasets_match_reader_labels():
    """Each `BULK_DATASETS` entry names the annotation its reader resolves."""
    assert BULK_DATASETS == {
        "crs": (CRS_FLOW_URL, CRS_BULK_LABEL),
        "crs-reduced": (CRS_FLOW_URL, CRS_REDUCED_BULK_LABEL),
        "dac1": (DAC1_FLOW_URL, DAC1_BULK_LABEL),
        "dac2a": (DAC2A_FLOW_URL, DAC2A_BULK_LABEL),
        "dac2b": (DAC2B_FLOW_URL, DAC2B_BULK_LABEL),
        "multisystem": (MULTI_FLOW_URL, MULTISYSTEM_BULK_LABEL),
    }


@pytest.mark.unit
@pytest.mark.parametrize(
    ("module", "reader", "flow_url", "label"),
    [
        ("crs", "bulk_download_crs", CRS_FLOW_URL, CRS_BULK_LABEL),
        ("dac1", "bulk_download_dac1", DAC1_FLOW_URL, DAC1_BULK_LABEL),
        ("dac2a", "bulk_download_dac2a", DAC2A_FLOW_URL, DAC2A_BULK_LABEL),
        ("dac2b", "bulk_download_dac2b", DAC2B_FLOW_URL, DAC2B_BULK_LABEL),
        (
            "multisystem",
            "bulk_download_multisystem",
            MULTI_FLOW_URL,
            MULTISYSTEM_BULK_LABEL,
        ),
    ],
)
def test_offline_reader_skips_resolution(mocker, module, reader, flow_url, label):
    import importlib

    mod = importlib.import_module(f"oda_reader.{module}")
    mocker.patch.object(
        mod,
        "get_bulk_file_url_with_version",
        side_effect=AssertionError("URL resolution attempted offline"),
    )
    bulk = mocker.patch.object(mod, "bulk_download_parquet", return_value=None)

    getattr(mod, reader)(offline=True)

    kwargs = bulk.call_args.kwargs
    assert (kwargs["url"], kwargs["flow_url"], kwargs["label"], kwargs["offline"]) == (
        None,
        flow_url,
        label,
        True,
    )
