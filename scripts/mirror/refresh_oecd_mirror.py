"""Refresh the R2 mirror of OECD bulk files behind bulk.developmentfinance.org.

For each dataset in ``BULK_DATASETS`` the script resolves OECD's bulk-file
URL and probes its version token. When the token matches the manifest entry,
it stamps the entry's ``checked_at``. Otherwise it downloads the file from
OECD, uploads it to ``oecd/<dataset>/<sha256><ext>`` and replaces the entry.
It writes the manifest last, then deletes each replaced dataset's objects
other than the current and the previous one.

A dataset OECD challenges, or that fails for any other reason, keeps its
entry unchanged, ``checked_at`` included. The script refreshes the other
datasets, writes the manifest, and exits 1.

The current manifest is read through the S3 API, so a run works while the
Worker serving the public URL is down.

Usage:
    uv run --group mirror python -m scripts.mirror.refresh_oecd_mirror
    uv run --group mirror python -m scripts.mirror.refresh_oecd_mirror --dry-run
    uv run --group mirror python -m scripts.mirror.refresh_oecd_mirror \\
        --dataset dac1 --dataset dac2a

Environment:
    R2_ACCOUNT_ID: Cloudflare account id; the S3 endpoint is
        ``https://<R2_ACCOUNT_ID>.r2.cloudflarestorage.com``.
    R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY: An R2 API token with Object Read
        & Write on the bucket.
    R2_BUCKET: The bucket name, ``developmentfinance-bulk`` when unset.
"""

import argparse
import dataclasses
import logging
import os
import re
import sys
import tempfile
import typing
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit

from oda_reader.download.download_tools import (
    BULK_DATASETS,
    _fetch_revalidation_token,
    _resolve_bulk_file,
    _stream_to_file,
)
from oda_reader.download.mirror import (
    MANIFEST_KEY,
    MIRROR_ENV_VAR,
    OBJECT_PREFIX,
    MirrorFile,
    MirrorManifest,
    dump_manifest,
    file_sha256,
    parse_manifest,
)

logger = logging.getLogger("refresh_oecd_mirror")

ATTRIBUTION = (
    "Source: OECD, Development Assistance Committee statistics, "
    "https://www.oecd.org/en/data/. Copied unchanged by ONE Campaign; "
    "OECD does not endorse this copy."
)
DEFAULT_BUCKET = "developmentfinance-bulk"

_OBJECT_CACHE_CONTROL = "public, max-age=31536000, immutable"
_MANIFEST_CACHE_CONTROL = "public, max-age=300"
_CONTENT_TYPES = {
    ".zip": "application/zip",
    ".parquet": "application/vnd.apache.parquet",
    ".csv": "text/csv",
}
_EXTENSION_RE = re.compile(r"\.[a-z0-9]+")
# The headers `download_tools._ensure_cached_bulk_file` sends, so the mirror
# holds the bytes a reader would have cached.
_DOWNLOAD_HEADERS = {"Accept-Encoding": "gzip"}

# A boto3 S3 client. boto3 ships no type information, so the alias only
# documents intent.
type S3Client = typing.Any


class RefreshError(Exception):
    """OECD's response or the stored object cannot back a manifest entry."""


@dataclasses.dataclass(frozen=True, slots=True)
class _Replacement:
    """A dataset whose object changed, with the objects its prune keeps."""

    dataset: str
    keep: frozenset[str]


def _now() -> datetime:
    return datetime.now(UTC)


# ---------------------------------------------------------------------------
# Refresh
# ---------------------------------------------------------------------------


def refresh_mirror(
    client: S3Client,
    *,
    bucket: str,
    datasets: typing.Sequence[str],
    dry_run: bool,
) -> list[str]:
    """Refresh *datasets* in the mirror and return the names that failed.

    Args:
        client: A boto3 S3 client pointed at R2.
        bucket: The bucket holding the mirror.
        datasets: `BULK_DATASETS` names to refresh, in order.
        dry_run: If True, log what would change and write nothing.

    Returns:
        The datasets that failed, in run order. Their entries are unchanged.

    Raises:
        MirrorManifestError: If the bucket's manifest exists but is invalid.
            Nothing is written.
    """
    current = _read_manifest(client, bucket=bucket)
    files = dict(current.files) if current is not None else {}
    replacements: list[_Replacement] = []
    failed: list[str] = []

    for dataset in datasets:
        try:
            replacement = _refresh_dataset(
                client, bucket=bucket, dataset=dataset, files=files, dry_run=dry_run
            )
        except Exception:
            # One dataset's failure does not stop the others. The run still
            # exits non-zero.
            logger.exception(f"{dataset}: refresh failed; its entry is unchanged.")
            failed.append(dataset)
            continue
        if replacement is not None:
            replacements.append(replacement)

    if dry_run:
        logger.info("Dry run: wrote nothing.")
        return failed

    manifest = MirrorManifest(generated_at=_now(), attribution=ATTRIBUTION, files=files)
    client.put_object(
        Bucket=bucket,
        Key=MANIFEST_KEY,
        Body=dump_manifest(manifest).encode(),
        ContentType="application/json",
        CacheControl=_MANIFEST_CACHE_CONTROL,
    )
    logger.info(f"Wrote {MANIFEST_KEY} with {len(files)} entries.")

    for replacement in replacements:
        _prune(client, bucket=bucket, replacement=replacement)
    return failed


def _refresh_dataset(
    client: S3Client,
    *,
    bucket: str,
    dataset: str,
    files: dict[str, MirrorFile],
    dry_run: bool,
) -> _Replacement | None:
    """Bring *dataset*'s entry in *files* up to date with OECD.

    Updates *files* in place. Returns the replacement when a new object
    backs the entry, or None when the entry was only stamped.

    Raises:
        RefreshError: If OECD answers without a version token, or the
            bucket does not hold the entry's object at the downloaded size.
    """
    flow_url, label = BULK_DATASETS[dataset]
    url, label_release = _resolve_bulk_file(
        flow_url, label, bypass_annotation_cache=True
    )
    current = files.get(url)

    probed = _fetch_revalidation_token(url)
    if probed is None:
        raise RefreshError(f"OECD sent no version token for {url}.")

    if current is not None and current.token == probed:
        files[url] = dataclasses.replace(current, checked_at=_now())
        logger.info(f"{dataset}: unchanged at {probed}; stamped checked_at.")
        return None

    if dry_run:
        held = current.token if current is not None else "nothing"
        logger.info(f"{dataset}: would download {url} ({probed}; mirror holds {held}).")
        return None

    started = _now()
    extension = _extension(url)
    with tempfile.TemporaryDirectory(prefix="oda-mirror-") as tmp:
        path = Path(tmp) / f"download{extension}"
        validator = _stream_to_file(url, _DOWNLOAD_HEADERS, path)
        if validator is None:
            raise RefreshError(
                f"OECD served {url} with neither ETag nor Last-Modified."
            )
        token, token_kind = validator
        sha256 = file_sha256(path)
        size = path.stat().st_size
        key = f"{OBJECT_PREFIX}{dataset}/{sha256}{extension}"
        if current is not None and current.object == key:
            logger.info(f"{dataset}: same bytes under a new token; skipping upload.")
        else:
            client.upload_file(
                str(path),
                bucket,
                key,
                ExtraArgs={
                    "ContentType": _CONTENT_TYPES.get(
                        extension, "application/octet-stream"
                    ),
                    "CacheControl": _OBJECT_CACHE_CONTROL,
                },
            )
            logger.info(f"{dataset}: uploaded {key} ({size} bytes).")
        _confirm_object(client, bucket=bucket, key=key, size=size)

    # Entries for this dataset at another URL are left over from a URL OECD
    # retired. The new entry replaces them, and their objects count as the
    # previous one for pruning.
    dataset_prefix = f"{OBJECT_PREFIX}{dataset}/"
    previous = {u: e for u, e in files.items() if e.object.startswith(dataset_prefix)}
    for old_url in previous:
        del files[old_url]

    files[url] = MirrorFile(
        token=token,
        token_kind=token_kind,
        # The annotation's release names the probed file; bytes under any
        # other token may be a different release.
        label_release=label_release if token == probed else None,
        object=key,
        size=size,
        sha256=sha256,
        checked_at=started,
    )
    logger.info(f"{dataset}: mirror now holds {token} for {url}.")
    keep = frozenset({key, *(e.object for e in previous.values())})
    return _Replacement(dataset=dataset, keep=keep)


def _confirm_object(client: S3Client, *, bucket: str, key: str, size: int) -> None:
    """Check that *bucket* holds *key* at *size* bytes before an entry names it.

    The upload sends no checksum, and a skipped upload assumes the object is
    still there, so the stored object is read back here.

    Raises:
        RefreshError: If the object is missing or has another size.
    """
    try:
        head = client.head_object(Bucket=bucket, Key=key)
    except client.exceptions.ClientError as e:
        raise RefreshError(f"{bucket} does not hold {key}: {e}") from e
    if head["ContentLength"] != size:
        raise RefreshError(
            f"{bucket} holds {key} at {head['ContentLength']} bytes; "
            f"the download was {size}."
        )


def _extension(url: str) -> str:
    """The lowercase suffix of *url*'s path, or "" when the suffix is not alphanumeric."""
    suffix = Path(urlsplit(url).path).suffix.lower()
    return suffix if _EXTENSION_RE.fullmatch(suffix) else ""


# ---------------------------------------------------------------------------
# Bucket access
# ---------------------------------------------------------------------------


def _read_manifest(client: S3Client, *, bucket: str) -> MirrorManifest | None:
    """The bucket's manifest, or None when the bucket has none yet."""
    try:
        response = client.get_object(Bucket=bucket, Key=MANIFEST_KEY)
    except client.exceptions.NoSuchKey:
        logger.info(f"No {MANIFEST_KEY} in {bucket}; starting an empty manifest.")
        return None
    return parse_manifest(response["Body"].read())


def _prune(client: S3Client, *, bucket: str, replacement: _Replacement) -> None:
    """Delete the dataset's objects other than those *replacement* keeps."""
    prefix = f"{OBJECT_PREFIX}{replacement.dataset}/"
    paginator = client.get_paginator("list_objects_v2")
    stale = [
        item["Key"]
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix)
        for item in page.get("Contents", [])
        if item["Key"] not in replacement.keep
    ]
    for key in stale:
        client.delete_object(Bucket=bucket, Key=key)
        logger.info(f"{replacement.dataset}: deleted {key}.")


def _client_from_env() -> S3Client:
    """Build an S3 client for R2 from the ``R2_*`` environment variables."""
    import boto3  # noqa: PLC0415 - the `mirror` group only; tests inject a fake
    from botocore.config import Config  # noqa: PLC0415 - as above

    return boto3.client(
        "s3",
        endpoint_url=f"https://{_require_env('R2_ACCOUNT_ID')}.r2.cloudflarestorage.com",
        aws_access_key_id=_require_env("R2_ACCESS_KEY_ID"),
        aws_secret_access_key=_require_env("R2_SECRET_ACCESS_KEY"),
        region_name="auto",
        # R2 rejects some of the default checksum headers newer botocore sends.
        config=Config(
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
        ),
    )


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise SystemExit(f"Set {name} to run the mirror refresher.")
    return value


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    """Run the refresher and return the process exit code."""
    parser = argparse.ArgumentParser(
        description="Refresh the R2 mirror of OECD bulk files."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log what would change and write nothing to the bucket.",
    )
    parser.add_argument(
        "--dataset",
        action="append",
        choices=sorted(BULK_DATASETS),
        help="Refresh only this dataset. Repeat for more. Default: all.",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    # The refresher reads OECD itself, bypassing its own mirror.
    os.environ[MIRROR_ENV_VAR] = "off"

    failed = refresh_mirror(
        _client_from_env(),
        bucket=os.environ.get("R2_BUCKET") or DEFAULT_BUCKET,
        datasets=args.dataset or list(BULK_DATASETS),
        dry_run=args.dry_run,
    )
    if failed:
        logger.error(f"Failed datasets: {', '.join(failed)}.")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
