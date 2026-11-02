"""Tests for the bulk-file mirror core in `oda_reader.download.mirror`."""

import copy
import dataclasses
import hashlib
import json
import logging
import threading
from datetime import UTC, datetime

import pytest
import requests

from oda_reader.download import mirror
from oda_reader.download.download_tools import _stream_to_file
from oda_reader.download.mirror import (
    DEFAULT_MIRROR_BASE_URL,
    MirrorIntegrityError,
    MirrorManifestError,
    dump_manifest,
    fetch_manifest,
    mirror_base_url,
    mirror_may_replace,
    mirror_url_validator,
    object_url,
    parse_manifest,
)

_BASE = "https://bulk.example.org/oecd/"
_OECD_URL = "https://webfs-dcd.oecd.org/files/dotStat/DSD_CRS/CRS%202022.zip"
_PAYLOAD = b"mirrored bytes"
_SHA = hashlib.sha256(_PAYLOAD).hexdigest()
_MANIFEST = {
    "schema": 1,
    "generated_at": "2026-11-02T15:00:00Z",
    "attribution": "Source: OECD.",
    "files": {
        _OECD_URL: {
            "token": "0x8DF2533665E7CB7",
            "token_kind": "etag",
            "label_release": "v20261006",
            "object": f"oecd/crs/{_SHA}.zip",
            "size": len(_PAYLOAD),
            "sha256": _SHA,
            "checked_at": "2026-11-02T15:00:00Z",
        }
    },
}
_CHECKED_AT = datetime(2026, 11, 2, 15, 0, tzinfo=UTC)


@pytest.fixture(autouse=True)
def _fresh_manifest_cache():
    mirror._clear_manifest_cache()
    yield
    mirror._clear_manifest_cache()


def _entry(**changes):
    entry = parse_manifest(json.dumps(_MANIFEST)).files[_OECD_URL]
    return dataclasses.replace(entry, **changes)


class _FakeResponse:
    def __init__(self, status_code, body=b"", headers=None):
        self.status_code = status_code
        self.headers = headers or {}
        self.encoding = None
        self._body = body

    def iter_content(self, chunk_size=8192):
        return iter([self._body] if self._body else [])

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False


def _patch_session(mocker, module, *responses):
    session = mocker.Mock()
    session.get.side_effect = list(responses)
    mocker.patch(f"{module}._get_bulk_stream_session", return_value=session)
    return session


@pytest.mark.unit
class TestMirrorBaseUrl:
    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            (None, DEFAULT_MIRROR_BASE_URL),
            ("", DEFAULT_MIRROR_BASE_URL),
            ("off", None),
            (" OFF ", None),
            ("https://m.example.org/oecd", "https://m.example.org/oecd/"),
            ("https://m.example.org/oecd/", "https://m.example.org/oecd/"),
        ],
    )
    def test_reads_env(self, monkeypatch, value, expected):
        if value is None:
            monkeypatch.delenv(mirror.MIRROR_ENV_VAR, raising=False)
        else:
            monkeypatch.setenv(mirror.MIRROR_ENV_VAR, value)
        assert mirror_base_url() == expected

    @pytest.mark.parametrize(
        "value", ["http://m.example.org/oecd/", "m.example.org", "https://m/x?a=1"]
    )
    def test_rejects_non_https_base(self, monkeypatch, value):
        monkeypatch.setenv(mirror.MIRROR_ENV_VAR, value)
        with pytest.raises(ValueError, match=mirror.MIRROR_ENV_VAR):
            mirror_base_url()


@pytest.mark.unit
class TestManifestModel:
    def test_round_trip(self):
        manifest = parse_manifest(json.dumps(_MANIFEST))

        assert json.loads(dump_manifest(manifest)) == _MANIFEST
        assert parse_manifest(dump_manifest(manifest)) == manifest
        assert manifest.files[_OECD_URL].checked_at == _CHECKED_AT

    def test_ignores_unknown_fields(self):
        data = copy.deepcopy(_MANIFEST)
        data["extra"] = 1
        data["files"][_OECD_URL]["extra"] = "x"

        assert parse_manifest(json.dumps(data)) == parse_manifest(json.dumps(_MANIFEST))

    @pytest.mark.parametrize(
        ("path", "value"),
        [
            (("schema",), 2),
            (("schema",), True),
            (("generated_at",), "2026-11-02T15:00:00"),
            (("files",), []),
            (("files", _OECD_URL, "token"), ""),
            (("files", _OECD_URL, "token"), None),
            (("files", _OECD_URL, "token_kind"), "md5"),
            (("files", _OECD_URL, "label_release"), 20261006),
            (("files", _OECD_URL, "size"), -1),
            (("files", _OECD_URL, "size"), True),
            (("files", _OECD_URL, "sha256"), _SHA.upper()),
            (("files", _OECD_URL, "object"), f"oecd/crs/{'0' * 64}.zip"),
            (("files", _OECD_URL, "object"), f"other/crs/{_SHA}.zip"),
            (("files", _OECD_URL, "object"), f"oecd/../{_SHA}.zip"),
            (("files", _OECD_URL, "checked_at"), "yesterday"),
        ],
    )
    def test_rejects_invalid_field(self, path, value):
        data = copy.deepcopy(_MANIFEST)
        target = data
        for key in path[:-1]:
            target = target[key]
        target[path[-1]] = value

        with pytest.raises(MirrorManifestError):
            parse_manifest(json.dumps(data))

    def test_rejects_missing_label_release(self):
        data = copy.deepcopy(_MANIFEST)
        del data["files"][_OECD_URL]["label_release"]

        with pytest.raises(MirrorManifestError):
            parse_manifest(json.dumps(data))

    @pytest.mark.parametrize(
        "text",
        [
            "not json",
            "[]",
            json.dumps({**_MANIFEST, "files": {"http://x.oecd.org/f": {}}}),
            json.dumps({**_MANIFEST, "files": {"https://[": {}}}),
        ],
    )
    def test_rejects_malformed_document(self, text):
        with pytest.raises(MirrorManifestError):
            parse_manifest(text)


@pytest.mark.unit
class TestMirrorMayReplace:
    @pytest.mark.parametrize(
        ("record", "announced", "expected"),
        [
            (None, None, True),
            ({"version": "old", "downloaded_at": "2026-11-03T00:00:00+0000"},
             "0x8DF2533665E7CB7", True),
            ({"version": "0x8DF2533665E7CB7",
              "downloaded_at": "2026-11-03T00:00:00+0000"}, None, True),
            ({"version": "old", "content_hash": f"sha256:{_SHA}",
              "downloaded_at": "2026-11-03T00:00:00+0000"}, None, True),
            ({"version": "old", "downloaded_at": "2026-11-02T14:59:59+0000"},
             None, True),
            ({"version": "old", "downloaded_at": "2026-11-02T15:00:00+0000"},
             None, False),
            ({"version": "old", "downloaded_at": "2026-11-03T00:00:00+0000"},
             "newer", False),
        ],
    )  # fmt: skip
    def test_rule(self, record, announced, expected):
        assert mirror_may_replace(record, _entry(), announced) is expected


@pytest.mark.unit
class TestMirrorUrls:
    def test_object_url_passes_validator(self):
        url = object_url(_entry(), base=_BASE)

        assert url == f"{_BASE}crs/{_SHA}.zip"
        mirror_url_validator(base=_BASE)(url)

    @pytest.mark.parametrize(
        "url",
        [
            f"http://bulk.example.org/oecd/crs/{_SHA}.zip",
            f"https://bulk.example.org:8443/oecd/crs/{_SHA}.zip",
            f"https://evil.example.org/oecd/crs/{_SHA}.zip",
            f"https://bulk.example.org/oecdx/crs/{_SHA}.zip",
            "https://bulk.example.org/other.zip",
            "https://bulk.example.org/oecd/../secret",
            "https://bulk.example.org/oecd/%2e%2e/secret",
            _OECD_URL,
        ],
    )
    def test_validator_rejects_off_mirror_url(self, url):
        with pytest.raises(ValueError, match="outside the mirror"):
            mirror_url_validator(base=_BASE)(url)


@pytest.mark.unit
class TestStreamToFileValidate:
    """`_stream_to_file(validate=...)` swaps the host check for every hop."""

    _MIRROR_URL = f"{_BASE}crs/{_SHA}.zip"

    @pytest.fixture(autouse=True)
    def _no_rate_limit(self, mocker):
        mocker.patch("oda_reader.download.download_tools.API_RATE_LIMITER.wait")

    def test_mirror_validator_admits_mirror_url(self, mocker, tmp_path):
        _patch_session(
            mocker,
            "oda_reader.download.download_tools",
            _FakeResponse(200, _PAYLOAD, headers={"ETag": '"t"'}),
        )
        target = tmp_path / "f.zip"

        _stream_to_file(
            self._MIRROR_URL, {}, target, validate=mirror_url_validator(base=_BASE)
        )

        assert target.read_bytes() == _PAYLOAD

    def test_default_validator_still_refuses_mirror_url(self, mocker, tmp_path):
        session = _patch_session(mocker, "oda_reader.download.download_tools")

        with pytest.raises(ValueError, match="untrusted host"):
            _stream_to_file(self._MIRROR_URL, {}, tmp_path / "f.zip")
        session.get.assert_not_called()

    def test_mirror_validator_refuses_redirect_off_mirror(self, mocker, tmp_path):
        session = _patch_session(
            mocker,
            "oda_reader.download.download_tools",
            _FakeResponse(302, headers={"Location": _OECD_URL}),
        )

        with pytest.raises(ValueError, match="outside the mirror"):
            _stream_to_file(
                self._MIRROR_URL,
                {},
                tmp_path / "f.zip",
                validate=mirror_url_validator(base=_BASE),
            )
        assert session.get.call_count == 1


@pytest.mark.unit
class TestFetchManifest:
    _MODULE = "oda_reader.download.mirror"

    def test_fetches_once_within_ttl(self, mocker):
        session = _patch_session(
            mocker, self._MODULE, _FakeResponse(200, json.dumps(_MANIFEST).encode())
        )

        first = fetch_manifest(base=_BASE)
        second = fetch_manifest(base=_BASE)

        assert first is not None
        assert first.files[_OECD_URL].sha256 == _SHA
        assert second is first
        session.get.assert_called_once()
        assert session.get.call_args.args[0] == f"{_BASE}manifest.json"

    def test_refetches_after_ttl(self, mocker):
        body = json.dumps(_MANIFEST).encode()
        session = _patch_session(
            mocker, self._MODULE, _FakeResponse(200, body), _FakeResponse(200, body)
        )
        clock = mocker.patch(f"{self._MODULE}.time.monotonic", return_value=1000.0)

        fetch_manifest(base=_BASE)
        clock.return_value = 1000.0 + mirror._MANIFEST_TTL_SECONDS
        fetch_manifest(base=_BASE)

        assert session.get.call_count == 2

    @pytest.mark.parametrize(
        "outcome",
        [
            _FakeResponse(404, b"missing"),
            _FakeResponse(302, headers={"Location": "https://elsewhere.org/m.json"}),
            _FakeResponse(200, b"{not json"),
            _FakeResponse(
                200, json.dumps({**_MANIFEST, "files": {"https://[": {}}}).encode()
            ),
            requests.ConnectionError("down"),
        ],
    )
    def test_failure_returns_none_and_warns_once(self, mocker, caplog, outcome):
        session = _patch_session(mocker, self._MODULE, outcome)

        with caplog.at_level(logging.WARNING, logger="oda_reader"):
            assert fetch_manifest(base=_BASE) is None
            assert fetch_manifest(base=_BASE) is None

        session.get.assert_called_once()
        assert len([r for r in caplog.records if r.levelname == "WARNING"]) == 1

    def test_oversized_manifest_returns_none(self, mocker):
        mocker.patch(f"{self._MODULE}._MANIFEST_MAX_BYTES", 10)
        _patch_session(
            mocker, self._MODULE, _FakeResponse(200, json.dumps(_MANIFEST).encode())
        )

        assert fetch_manifest(base=_BASE) is None


@pytest.mark.unit
class TestStreamToFileVerify:
    """`_stream_to_file(verify=...)` checks the bytes as they are written."""

    @pytest.fixture(autouse=True)
    def _no_rate_limit(self, mocker):
        mocker.patch("oda_reader.download.download_tools.API_RATE_LIMITER.wait")

    def test_verify_gets_the_streamed_size_and_digest(self, mocker, tmp_path):
        _patch_session(
            mocker, "oda_reader.download.download_tools", _FakeResponse(200, _PAYLOAD)
        )
        verify = mocker.Mock()

        _stream_to_file(_OECD_URL, {}, tmp_path / "f.zip", verify=verify)

        verify.assert_called_once_with(len(_PAYLOAD), _SHA)

    @pytest.mark.parametrize(
        "payload", [_PAYLOAD + b"!", b"X" + _PAYLOAD[1:]], ids=["size", "sha256"]
    )
    def test_mismatch_raises_without_retrying(self, mocker, tmp_path, payload):
        session = _patch_session(
            mocker, "oda_reader.download.download_tools", _FakeResponse(200, payload)
        )

        with pytest.raises(MirrorIntegrityError):
            _stream_to_file(
                _OECD_URL,
                {},
                tmp_path / "f.zip",
                verify=lambda size, sha256: mirror.verify_mirror_copy(
                    _entry(), size=size, sha256=sha256
                ),
            )
        session.get.assert_called_once()


@pytest.mark.unit
class TestFetchManifestConcurrency:
    """A manifest fetch blocks only threads waiting for the same base."""

    _OTHER_BASE = "https://other.example.org/oecd/"

    def test_slow_base_does_not_block_another(self, mocker):
        release_slow = threading.Event()
        calls: list[str] = []

        def get(url, **_kwargs):
            calls.append(url)
            if url.startswith(_BASE):
                release_slow.wait(timeout=10)
            return _FakeResponse(200, json.dumps(_MANIFEST).encode())

        session = mocker.Mock()
        session.get.side_effect = get
        mocker.patch(
            "oda_reader.download.mirror._get_bulk_stream_session", return_value=session
        )
        slow = [
            threading.Thread(target=fetch_manifest, kwargs={"base": _BASE})
            for _ in range(2)
        ]
        for thread in slow:
            thread.start()

        other = threading.Thread(
            target=fetch_manifest, kwargs={"base": self._OTHER_BASE}
        )
        other.start()
        other.join(timeout=5)
        finished_while_slow_fetch_ran = not other.is_alive()
        release_slow.set()
        for thread in slow:
            thread.join(timeout=5)

        assert finished_while_slow_fetch_ran
        # The two threads asking for the slow base shared one fetch.
        assert calls.count(f"{_BASE}manifest.json") == 1
