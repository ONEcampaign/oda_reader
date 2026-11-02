"""Tests for the bulk-file mirror standing in for a challenged OECD download.

A fake network stands behind the one requests session every bulk request
uses, so each test sees exactly which URLs were requested.
"""

import hashlib
import io
import json
import logging
from datetime import UTC, datetime, timedelta

import pandas as pd
import pytest
import requests

from oda_reader import bulk_download_crs, release
from oda_reader._cache.manager import ISO_FORMAT, CacheManager, bulk_cache_manager
from oda_reader.crs import CRS_BULK_LABEL
from oda_reader.download import download_tools
from oda_reader.download.download_tools import (
    CRS_FLOW_URL,
    _ensure_cached_bulk_file,
    bulk_download_parquet,
)
from oda_reader.download.mirror import MIRROR_ENV_VAR
from oda_reader.exceptions import BulkDownloadChallengeError

_OECD_URL = "https://webfs-dcd.oecd.org/files/dotStat/DSD_CRS/CRS.parquet"
_BASE = "https://mirror.example.org/oecd/"
_MIRROR_TOKEN = '"oecd-2"'


def _parquet(rows: int) -> bytes:
    buffer = io.BytesIO()
    pd.DataFrame({"donor_code": range(rows)}).to_parquet(buffer)
    return buffer.getvalue()


_OLD = _parquet(2)
_NEW = _parquet(3)


class _Response:
    def __init__(self, status_code, body=b"", headers=None):
        self.status_code = status_code
        self.headers = headers or {}
        self.encoding = None
        self._body = body

    def iter_content(self, chunk_size=8192):
        return iter([self._body] if self._body else [])

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False


class FakeNet:
    """OECD's file host and the mirror, behind the shared bulk session.

    OECD challenges every request unless `oecd_token` is set, in which case
    it serves `oecd_body` under that token. `probe_failure`, when set, is
    the HTTP status or exception OECD answers the ranged version probe with
    instead. The mirror serves `manifest` and the bodies in `objects`, keyed
    by URL; anything else is a 404.
    """

    def __init__(self, mocker) -> None:
        self.requested: list[str] = []
        self.oecd_token: str | None = None
        self.probe_failure: int | Exception | None = None
        self.oecd_body = _OLD
        self.manifest: dict | None = None
        self.objects: dict[str, bytes] = {}
        session = mocker.Mock()
        session.get.side_effect = self._get
        for module in ("download_tools", "mirror"):
            mocker.patch(
                f"oda_reader.download.{module}._get_bulk_stream_session",
                return_value=session,
            )
        mocker.patch("oda_reader.download.download_tools.API_RATE_LIMITER.wait")

    def mirror_requests(self) -> list[str]:
        return [url for url in self.requested if url.startswith(_BASE)]

    def object_requests(self) -> list[str]:
        return [url for url in self.mirror_requests() if url in self.objects]

    def publish(
        self,
        body: bytes = _NEW,
        *,
        token: str = _MIRROR_TOKEN,
        checked_at: datetime | None = None,
        served: bytes | None = None,
    ) -> str:
        """List *body* in the manifest; serve *served* (default *body*) for it."""
        sha = hashlib.sha256(body).hexdigest()
        checked_at = checked_at or datetime.now(UTC) - timedelta(days=1)
        self.manifest = {
            "schema": 1,
            "generated_at": "2026-11-02T15:00:00Z",
            "attribution": "OECD",
            "files": {
                _OECD_URL: {
                    "token": token,
                    "token_kind": "etag",
                    "label_release": "v20261006",
                    "object": f"oecd/crs/{sha}.parquet",
                    "size": len(body),
                    "sha256": sha,
                    "checked_at": checked_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                }
            },
        }
        url = f"{_BASE}crs/{sha}.parquet"
        self.objects[url] = body if served is None else served
        return url

    def _get(self, url, headers=None, **_kwargs):
        self.requested.append(url)
        if url == _OECD_URL and "Range" in (headers or {}):
            if isinstance(self.probe_failure, Exception):
                raise self.probe_failure
            if self.probe_failure is not None:
                return _Response(self.probe_failure, b"Service unavailable")
        if url == _OECD_URL:
            if self.oecd_token is None:
                return _Response(
                    403, b"<html>", {"cf-mitigated": "challenge", "cf-ray": "ray-1"}
                )
            return _Response(200, self.oecd_body, {"ETag": self.oecd_token})
        if url == f"{_BASE}manifest.json" and self.manifest is not None:
            return _Response(200, json.dumps(self.manifest).encode())
        if url in self.objects:
            # R2's own ETag, which must never be recorded as the version.
            return _Response(200, self.objects[url], {"ETag": '"r2-etag"'})
        return _Response(404, b"Not found")


@pytest.fixture
def net(mocker, monkeypatch):
    monkeypatch.setenv(MIRROR_ENV_VAR, _BASE)
    return FakeNet(mocker)


def _ensure(version: str | None, *, challenged: bool = False):
    return _ensure_cached_bulk_file(
        _OECD_URL,
        version,
        flow_url=CRS_FLOW_URL,
        label=CRS_BULK_LABEL,
        manager=bulk_cache_manager(),
        release="v20261006" if version is not None else None,
        challenged=challenged,
    )


def _rewrite_records(*, days_ago: float, **fields) -> None:
    """Date every cached record *days_ago* back and set *fields* on it."""
    manager = bulk_cache_manager()
    manifest = json.loads(manager.manifest_path.read_text())
    for record in manifest.values():
        downloaded = datetime.now(UTC) - timedelta(days=days_ago)
        record.update(fields, downloaded_at=downloaded.strftime(ISO_FORMAT))
    manager.manifest_path.write_text(json.dumps(manifest))


def _cache_from_oecd(net: FakeNet, token: str, *, days_ago: float = 0) -> None:
    """Download *token* from OECD, then move its record *days_ago* into the past."""
    net.oecd_token = token
    _ensure(token)
    net.oecd_token = None
    _rewrite_records(days_ago=days_ago)
    net.requested.clear()


def _cached_files() -> list[str]:
    return sorted(
        path.name
        for path in bulk_cache_manager().base_dir.iterdir()
        if path.name not in ("manifest.json", ".cache.lock")
    )


@pytest.mark.unit
class TestChallengedDownload:
    @pytest.mark.parametrize(
        ("announced", "challenged"), [(None, True), (_MIRROR_TOKEN, False)]
    )
    def test_mirror_serves_under_oecds_token(self, net, caplog, announced, challenged):
        net.publish(checked_at=datetime(2026, 11, 1, 9, 30, tzinfo=UTC))

        with caplog.at_level(logging.WARNING):
            path, record = _ensure(announced, challenged=challenged)

        assert path.read_bytes() == _NEW
        assert record["version"] == _MIRROR_TOKEN
        assert record["version_kind"] == "etag"
        assert record["release"] == "v20261006"
        assert record["source"] == "mirror"
        assert _BASE in caplog.text
        assert "2026-11-01 09:30" in caplog.text

    @pytest.mark.parametrize(
        "served",
        [_OLD, _NEW[:-1] + bytes([_NEW[-1] ^ 1])],
        ids=["size-mismatch", "hash-mismatch"],
    )
    def test_corrupt_mirror_copy_raises_the_challenge(self, net, served):
        net.publish(served=served)

        with pytest.raises(BulkDownloadChallengeError) as exc_info:
            _ensure(None)

        assert "manifest says" in "\n".join(exc_info.value.__notes__)
        assert bulk_cache_manager().list_records() == []
        assert _cached_files() == []

    def test_mirror_404_raises_the_challenge_not_a_404(self, net, mocker):
        url = net.publish()
        del net.objects[url]
        resolve = mocker.patch.object(download_tools, "_resolve_and_probe_bulk_file")

        with pytest.raises(BulkDownloadChallengeError) as exc_info:
            bulk_download_parquet(
                url=_OECD_URL, flow_url=CRS_FLOW_URL, label=CRS_BULK_LABEL
            )

        assert exc_info.value.status_code == 403
        assert "HTTP 404" in "\n".join(exc_info.value.__notes__)
        assert url in net.requested
        resolve.assert_not_called()
        assert bulk_cache_manager().list_records() == []

    def test_mirror_off_raises_the_challenge_without_asking_it(self, net, monkeypatch):
        monkeypatch.setenv(MIRROR_ENV_VAR, "off")
        net.publish()

        with pytest.raises(BulkDownloadChallengeError) as exc_info:
            _ensure(None)

        assert not hasattr(exc_info.value, "__notes__")
        assert net.mirror_requests() == []

    @pytest.mark.parametrize(
        "call",
        [lambda: _ensure('"oecd-1"'), lambda: release("crs")],
        ids=["ensure", "release"],
    )
    def test_invalid_mirror_setting_raises_before_any_request(
        self, net, monkeypatch, mocker, call
    ):
        monkeypatch.setenv(MIRROR_ENV_VAR, "http://mirror.example.org/oecd/")
        resolve = mocker.patch(
            "oda_reader._tables._resolve_bulk_file", return_value=(_OECD_URL, None)
        )

        with pytest.raises(ValueError, match=MIRROR_ENV_VAR):
            call()

        resolve.assert_not_called()
        assert net.requested == []

    def test_unchallenged_download_makes_no_mirror_request(self, net):
        net.publish()
        net.oecd_token = '"oecd-1"'

        _path, record = _ensure('"oecd-1"')

        assert record["source"] == "download"
        assert net.requested == [_OECD_URL]


@pytest.mark.unit
class TestChallengedProbe:
    """The version probe failed, so the mirror's token stands in for OECD's."""

    def test_cache_older_than_the_mirror_check_is_refetched(self, net):
        _cache_from_oecd(net, '"oecd-1"', days_ago=2)
        net.publish(checked_at=datetime.now(UTC) - timedelta(days=1))

        path, record = _ensure(None, challenged=True)

        assert path.read_bytes() == _NEW
        assert (record["version"], record["source"]) == (_MIRROR_TOKEN, "mirror")
        assert record["release"] == "v20261006"

    def test_cache_newer_than_the_mirror_check_is_kept(self, net):
        _cache_from_oecd(net, '"oecd-3"', days_ago=0)
        net.publish(checked_at=datetime.now(UTC) - timedelta(days=1))

        path, record = _ensure(None, challenged=True)

        assert path.read_bytes() == _OLD
        assert (record["version"], record["source"]) == ('"oecd-3"', "download")
        assert net.object_requests() == []

    def test_cache_holding_the_mirror_copy_by_hash_is_kept(self, net):
        _cache_from_oecd(net, '"oecd-1"', days_ago=2)
        net.publish(_OLD, checked_at=datetime.now(UTC) - timedelta(days=1))
        manager = bulk_cache_manager()
        key, _path, record = manager.find_record(
            key=download_tools._bulk_cache_key(_OECD_URL)
        )
        digest = hashlib.sha256(_OLD).hexdigest()
        manager.set_content_hash(
            key, f"sha256:{digest}", payload_id=record["payload_id"]
        )

        _path, record = _ensure(None, challenged=True)

        assert record["version"] == '"oecd-1"'
        assert net.object_requests() == []

    @pytest.mark.parametrize("cached", [False, True])
    def test_legacy_reader_reads_the_mirror_copy(self, net, mocker, cached):
        """A challenged probe reaches the cache through the public URL resolver."""
        mocker.patch.object(
            download_tools, "_resolve_bulk_file_url", return_value=_OECD_URL
        )
        if cached:
            _cache_from_oecd(net, '"oecd-1"', days_ago=2)
        net.publish()

        result = bulk_download_crs()

        pd.testing.assert_frame_equal(result, pd.read_parquet(io.BytesIO(_NEW)))
        [record] = bulk_cache_manager().list_records()
        assert (record["version"], record["source"]) == (_MIRROR_TOKEN, "mirror")


@pytest.mark.unit
def test_mirror_older_than_the_cache_is_not_used(net):
    """OECD announces a new file; the cache was downloaded after the mirror's check."""
    _cache_from_oecd(net, '"oecd-3"', days_ago=0)
    net.publish(checked_at=datetime.now(UTC) - timedelta(days=1))

    with pytest.raises(BulkDownloadChallengeError) as exc_info:
        _ensure('"oecd-4"')

    assert "may be older" in "\n".join(exc_info.value.__notes__)
    assert net.object_requests() == []
    [record] = bulk_cache_manager().list_records()
    assert record["version"] == '"oecd-3"'


@pytest.mark.unit
def test_lagging_mirror_holding_the_cached_copy_downloads_nothing(net):
    """OECD announces a newer file than the mirror's, which is already cached."""
    _cache_from_oecd(net, _MIRROR_TOKEN, days_ago=2)
    net.publish(_OLD)

    with pytest.raises(BulkDownloadChallengeError) as exc_info:
        _ensure('"oecd-3"')

    assert "already cached" in "\n".join(exc_info.value.__notes__)
    assert net.object_requests() == []
    [record] = bulk_cache_manager().list_records()
    assert record["version"] == _MIRROR_TOKEN


@pytest.mark.unit
class TestUncachedRead:
    @pytest.mark.parametrize(
        ("announced", "warns"), [(_MIRROR_TOKEN, False), ('"oecd-3"', True)]
    )
    def test_any_mirror_copy_serves(self, net, caplog, announced, warns):
        net.publish()

        with caplog.at_level(logging.WARNING):
            result = bulk_download_parquet(
                url=_OECD_URL, version=announced, use_raw_cache=False
            )

        pd.testing.assert_frame_equal(result, pd.read_parquet(io.BytesIO(_NEW)))
        assert ("not the" in caplog.text) is warns
        assert bulk_cache_manager().list_records() == []


@pytest.mark.unit
def test_re_resolve_after_a_404_passes_its_challenged_probe_on(net, mocker):
    """A retired URL's re-resolve probes the new URL; its challenge reaches the cache."""
    mocker.patch.object(
        download_tools, "_resolve_bulk_file_url", return_value=_OECD_URL
    )
    _cache_from_oecd(net, '"oecd-1"', days_ago=2)
    net.publish()
    retired = "https://webfs-dcd.oecd.org/files/dotStat/DSD_CRS/CRS-old.parquet"

    result = bulk_download_parquet(
        url=retired, flow_url=CRS_FLOW_URL, label=CRS_BULK_LABEL, version='"oecd-1"'
    )

    pd.testing.assert_frame_equal(result, pd.read_parquet(io.BytesIO(_NEW)))
    record = bulk_cache_manager().find_record(
        key=download_tools._bulk_cache_key(_OECD_URL)
    )[2]
    assert (record["version"], record["source"]) == (_MIRROR_TOKEN, "mirror")


@pytest.mark.unit
class TestUnchallengedProbe:
    """Users OECD does not challenge never ask the mirror."""

    @pytest.mark.parametrize(
        "failure", [503, requests.Timeout("probe timed out")], ids=["503", "timeout"]
    )
    def test_failed_probe_leaves_the_mirror_alone(self, net, mocker, failure):
        mocker.patch.object(
            download_tools, "_resolve_bulk_file_url", return_value=_OECD_URL
        )
        _cache_from_oecd(net, '"oecd-1"', days_ago=2)
        net.publish(checked_at=datetime.now(UTC) - timedelta(days=1))
        net.probe_failure = failure

        result = bulk_download_crs()

        pd.testing.assert_frame_equal(result, pd.read_parquet(io.BytesIO(_OLD)))
        assert net.mirror_requests() == []

    def test_direct_read_without_a_version_leaves_the_mirror_alone(self, net):
        net.publish()
        net.oecd_token = '"oecd-1"'

        bulk_download_parquet(url=_OECD_URL)

        [record] = bulk_cache_manager().list_records()
        assert record["source"] == "download"
        assert net.requested == [_OECD_URL]


@pytest.mark.unit
def test_mirror_decides_on_the_record_present_under_the_lock(net, mocker):
    """A newer OECD copy recorded after the caller first looked is kept."""
    _cache_from_oecd(net, '"oecd-1"', days_ago=2)
    net.publish(checked_at=datetime.now(UTC) - timedelta(days=1))
    ensure_record = CacheManager.ensure_record

    def newer_copy_lands_first(self, entry, **kwargs):
        _rewrite_records(days_ago=0, version='"oecd-3"')
        return ensure_record(self, entry, **kwargs)

    mocker.patch.object(
        CacheManager, "ensure_record", autospec=True, side_effect=newer_copy_lands_first
    )

    with pytest.raises(BulkDownloadChallengeError) as exc_info:
        _ensure('"oecd-4"')

    assert "may be older" in "\n".join(exc_info.value.__notes__)
    assert net.object_requests() == []
    [record] = bulk_cache_manager().list_records()
    assert record["version"] == '"oecd-3"'
