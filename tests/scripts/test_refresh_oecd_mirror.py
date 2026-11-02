"""Tests for the R2 mirror refresher in `scripts.mirror.refresh_oecd_mirror`.

OECD and R2 are faked: the package calls the script makes are replaced on its
module, and the S3 client is an in-memory bucket.
"""

# The fake S3 client takes boto3's CamelCase keyword arguments.
# ruff: noqa: N803

import dataclasses
import hashlib
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from oda_reader.download.download_tools import (
    BULK_DATASETS,
    _encode_url_path,
    _finalize_resolved_url,
)
from oda_reader.download.mirror import (
    MANIFEST_KEY,
    MIRROR_ENV_VAR,
    MirrorFile,
    MirrorManifest,
    dump_manifest,
    parse_manifest,
)
from oda_reader.exceptions import BulkDownloadChallengeError
from scripts.mirror import refresh_oecd_mirror as refresher

_BUCKET = "test-bucket"
_T0 = datetime(2026, 11, 1, 12, 0, tzinfo=UTC)


class _NoSuchKeyError(Exception):
    pass


class _ClientError(Exception):
    pass


class FakeS3:
    """An in-memory bucket with the boto3 calls the refresher makes."""

    class exceptions:  # noqa: N801 - mirrors boto3's `client.exceptions`
        NoSuchKey = _NoSuchKeyError
        ClientError = _ClientError

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.meta: dict[str, dict] = {}
        self.ops: list[tuple[str, str]] = []
        # Uploads under this key prefix lose their last byte in storage.
        self.truncate: str | None = None

    def get_object(self, *, Bucket, Key):
        assert Bucket == _BUCKET
        if Key not in self.objects:
            raise _NoSuchKeyError(Key)
        body = self.objects[Key]
        return {"Body": type("Body", (), {"read": lambda _self: body})()}

    def put_object(self, *, Bucket, Key, Body, ContentType, CacheControl):
        assert Bucket == _BUCKET
        self.objects[Key] = Body
        self.meta[Key] = {"ContentType": ContentType, "CacheControl": CacheControl}
        self.ops.append(("put", Key))

    def upload_file(self, Filename, Bucket, Key, ExtraArgs):
        assert Bucket == _BUCKET
        body = Path(Filename).read_bytes()
        if self.truncate is not None and Key.startswith(self.truncate):
            body = body[:-1]
        self.objects[Key] = body
        self.meta[Key] = ExtraArgs
        self.ops.append(("upload", Key))

    def head_object(self, *, Bucket, Key):
        assert Bucket == _BUCKET
        if Key not in self.objects:
            raise _ClientError(f"404 {Key}")
        return {"ContentLength": len(self.objects[Key])}

    def delete_object(self, *, Bucket, Key):
        assert Bucket == _BUCKET
        del self.objects[Key]
        self.ops.append(("delete", Key))

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        objects = self.objects

        class Paginator:
            def paginate(self, *, Bucket, Prefix):
                keys = sorted(k for k in objects if k.startswith(Prefix))
                # Two pages, so the refresher must walk every page.
                half = len(keys) // 2
                return [
                    {"Contents": [{"Key": k} for k in keys[:half]]},
                    {"Contents": [{"Key": k} for k in keys[half:]]},
                ]

        return Paginator()

    def manifest(self) -> MirrorManifest:
        return parse_manifest(self.objects[MANIFEST_KEY])

    def write_manifest(self, files: dict[str, MirrorFile]) -> None:
        manifest = MirrorManifest(generated_at=_T0, attribution="x", files=files)
        self.objects[MANIFEST_KEY] = dump_manifest(manifest).encode()


@dataclasses.dataclass
class FakeFile:
    """What fake OECD serves for one dataset."""

    url: str
    release: str | None
    probe: str | None
    body: bytes
    served: str | None = None  # token on the download; defaults to `probe`
    validator: bool = True  # whether the download carries ETag or Last-Modified
    challenge: bool = False


@dataclasses.dataclass
class FakeOecd:
    files: dict[str, FakeFile]
    downloads: list[str]


def _url(dataset: str) -> str:
    return _finalize_resolved_url(
        f"https://webfs-dcd.oecd.org/files/{dataset} data.zip"
    )


@pytest.fixture
def oecd(monkeypatch):
    """Fake OECD for every dataset, with a log of the downloads it served."""
    files = {
        name: FakeFile(
            url=_url(name), release="v20261006", probe=f'"{name}-1"', body=name.encode()
        )
        for name in BULK_DATASETS
    }
    by_url = {f.url: f for f in files.values()}
    downloads: list[str] = []

    def resolve(flow_url, label, latest_flow=None, *, bypass_annotation_cache=False):
        assert bypass_annotation_cache
        name = next(n for n, a in BULK_DATASETS.items() if a == (flow_url, label))
        return files[name].url, files[name].release

    def stream(url, headers, path, **kwargs):
        assert not kwargs, "the refresher must use the OECD validator"
        f = by_url[url]
        downloads.append(url)
        if f.challenge:
            raise BulkDownloadChallengeError(
                status_code=403, url=url, body="", cf_ray="abc"
            )
        path.write_bytes(f.body)
        return (f.served or f.probe, "etag") if f.validator else None

    monkeypatch.setattr(refresher, "_resolve_bulk_file", resolve)
    monkeypatch.setattr(
        refresher, "_fetch_revalidation_token", lambda url: by_url[url].probe
    )
    monkeypatch.setattr(refresher, "_stream_to_file", stream)
    ticks = iter(_T0 + timedelta(hours=i) for i in range(1, 1000))
    monkeypatch.setattr(refresher, "_now", lambda: next(ticks))

    return FakeOecd(files=files, downloads=downloads)


def _entry(dataset: str, body: bytes, *, token: str, checked_at=_T0) -> MirrorFile:
    sha = hashlib.sha256(body).hexdigest()
    return MirrorFile(
        token=token,
        token_kind="etag",
        label_release="v20260901",
        object=f"oecd/{dataset}/{sha}.zip",
        size=len(body),
        sha256=sha,
        checked_at=checked_at,
    )


def _run(s3, *datasets, dry_run=False):
    return refresher.refresh_mirror(
        s3, bucket=_BUCKET, datasets=list(datasets), dry_run=dry_run
    )


def test_new_file_is_uploaded_then_listed_under_its_oecd_url(oecd):
    s3 = FakeS3()

    assert _run(s3, "crs") == []

    url = oecd.files["crs"].url
    assert url == "https://webfs-dcd.oecd.org/files/crs%20data.zip"
    manifest = s3.manifest()
    assert list(manifest.files) == [_encode_url_path(url)]
    entry = manifest.files[url]
    sha = hashlib.sha256(b"crs").hexdigest()
    assert entry == MirrorFile(
        token='"crs-1"',
        token_kind="etag",
        label_release="v20261006",
        object=f"oecd/crs/{sha}.zip",
        size=3,
        sha256=sha,
        # The first clock reading is the download's start.
        checked_at=_T0 + timedelta(hours=1),
    )
    assert manifest.attribution == refresher.ATTRIBUTION
    assert s3.objects[entry.object] == b"crs"
    assert s3.meta[entry.object] == {
        "ContentType": "application/zip",
        "CacheControl": "public, max-age=31536000, immutable",
    }
    assert s3.meta[MANIFEST_KEY]["CacheControl"] == "public, max-age=300"
    assert s3.ops == [("upload", entry.object), ("put", MANIFEST_KEY)]


def test_unchanged_token_only_stamps_checked_at(oecd):
    s3 = FakeS3()
    url = oecd.files["dac1"].url
    old = _entry("dac1", b"dac1", token='"dac1-1"')
    s3.write_manifest({url: old})

    assert _run(s3, "dac1") == []

    assert oecd.downloads == []
    assert s3.manifest().files[url] == dataclasses.replace(
        old, checked_at=_T0 + timedelta(hours=1)
    )
    assert s3.ops == [("put", MANIFEST_KEY)]


def test_downloaded_token_differing_from_probe_drops_label_release(oecd):
    oecd.files["dac2a"].served = '"dac2a-2"'
    s3 = FakeS3()

    _run(s3, "dac2a")

    entry = s3.manifest().files[oecd.files["dac2a"].url]
    assert entry.token == '"dac2a-2"'
    assert entry.label_release is None


@pytest.mark.parametrize("missing", ["probe", "download"])
def test_missing_validator_refuses_the_entry(oecd, missing):
    f = oecd.files["dac2b"]
    if missing == "probe":
        f.probe = None
    else:
        f.validator = False
    s3 = FakeS3()

    assert _run(s3, "dac2b", "dac1") == ["dac2b"]

    files = s3.manifest().files
    assert f.url not in files
    assert oecd.files["dac1"].url in files
    assert not any(k.startswith("oecd/dac2b/") for k in s3.objects)


def test_challenge_leaves_entry_untouched_and_processes_the_rest(oecd):
    oecd.files["crs"].challenge = True
    oecd.files["crs"].probe = '"crs-2"'
    s3 = FakeS3()
    crs_url = oecd.files["crs"].url
    old = _entry("crs", b"old crs", token='"crs-1"')
    s3.write_manifest({crs_url: old})
    s3.objects[old.object] = b"old crs"

    assert _run(s3, "crs", "multisystem") == ["crs"]

    files = s3.manifest().files
    assert files[crs_url] == old
    assert oecd.files["multisystem"].url in files
    assert s3.objects[old.object] == b"old crs"


def test_replacement_prunes_objects_older_than_the_previous_one(oecd):
    oecd.files["crs"].probe = '"crs-3"'
    s3 = FakeS3()
    url = oecd.files["crs"].url
    previous = _entry("crs", b"crs v2", token='"crs-2"')
    older = _entry("crs", b"crs v1", token='"crs-1"')
    sibling = _entry("crs-reduced", b"reduced", token='"r-1"')
    s3.write_manifest({url: previous, oecd.files["crs-reduced"].url: sibling})
    for e, body in [(previous, b"crs v2"), (older, b"crs v1"), (sibling, b"reduced")]:
        s3.objects[e.object] = body

    _run(s3, "crs")

    current = s3.manifest().files[url]
    assert set(s3.objects) == {
        MANIFEST_KEY,
        current.object,
        previous.object,
        sibling.object,
    }
    # Objects are pruned only after the manifest naming the new one is written.
    assert s3.ops == [
        ("upload", current.object),
        ("put", MANIFEST_KEY),
        ("delete", older.object),
    ]


def test_moved_url_replaces_the_old_entry_and_keeps_its_object(oecd):
    s3 = FakeS3()
    retired = _url("dac1-old")
    old = _entry("dac1", b"old dac1", token='"dac1-0"')
    s3.write_manifest({retired: old})
    s3.objects[old.object] = b"old dac1"

    _run(s3, "dac1")

    files = s3.manifest().files
    assert list(files) == [oecd.files["dac1"].url]
    assert old.object in s3.objects


def test_dry_run_writes_and_downloads_nothing(oecd):
    s3 = FakeS3()
    url = oecd.files["dac1"].url
    s3.write_manifest({url: _entry("dac1", b"dac1", token='"dac1-1"')})
    before = dict(s3.objects)

    assert _run(s3, "dac1", "crs", dry_run=True) == []

    assert oecd.downloads == []
    assert s3.ops == []
    assert s3.objects == before


def test_main_turns_mirror_off_and_exits_non_zero_on_failure(oecd, monkeypatch):
    monkeypatch.setenv(MIRROR_ENV_VAR, "https://bulk.example.org/oecd/")
    monkeypatch.setenv("R2_BUCKET", _BUCKET)
    oecd.files["dac1"].challenge = True
    s3 = FakeS3()
    monkeypatch.setattr(refresher, "_client_from_env", lambda: s3)

    assert refresher.main(["--dataset", "dac1", "--dataset", "dac2a"]) == 1

    assert os.environ[MIRROR_ENV_VAR] == "off"
    assert list(s3.manifest().files) == [oecd.files["dac2a"].url]
    assert refresher.main(["--dataset", "dac2a"]) == 0


@pytest.mark.parametrize("fault", ["short-upload", "object-deleted"])
def test_entry_waits_for_its_object_in_the_bucket(oecd, fault):
    oecd.files["dac1"].probe = '"dac1-2"'
    s3 = FakeS3()
    url = oecd.files["dac1"].url
    old = _entry("dac1", b"dac1", token='"dac1-1"')
    s3.write_manifest({url: old})
    if fault == "short-upload":
        oecd.files["dac1"].body = b"dac1 v2"
        s3.truncate = "oecd/dac1/"
    # Otherwise OECD serves the same bytes under a new token, so the upload
    # is skipped, but the object was deleted from the bucket by hand.

    assert _run(s3, "dac1", "multisystem") == ["dac1"]

    files = s3.manifest().files
    assert files[url] == old
    assert oecd.files["multisystem"].url in files
