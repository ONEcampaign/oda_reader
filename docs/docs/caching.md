---
description: Configure API and bulk-file caches, change cache locations, clear stored data, and set request limits for OECD calls.
---

# Caching & Performance

ODA Reader uses caching to make repeated queries fast and reduce dependency on OECD's servers. This page explains how caching works and how to configure it.

## How caching works

ODA Reader caches three types of data:

1. **HTTP responses**: Raw API responses before processing
1. **DataFrames**: Processed pandas DataFrames after schema translation
1. **Bulk files**: Large parquet/zip files downloaded by `bulk_download_crs`,
   `download_crs_file`, `bulk_download_dac1`, `bulk_download_dac2a`, `bulk_download_dac2b`, and
   `bulk_download_multisystem`

All three caches are automatic and transparent. You don't need to change your code to benefit from caching.

**Example of caching in action**:

```python
from oda_reader import download_dac1
import time

# First call: hits the API (slow)
start = time.time()
data1 = download_dac1(start_year=2022, end_year=2022)
print(f"First call: {time.time() - start:.1f} seconds")

# Second call: uses cache (instant)
start = time.time()
data2 = download_dac1(start_year=2022, end_year=2022)
print(f"Second call: {time.time() - start:.1f} seconds")
```

**Typical output**:

```
First call: 15.3 seconds
Second call: 0.1 seconds
```

Cached queries are ~100x faster.

## Cache location

By default, caches are stored in a platform-specific directory resolved by `readerkit`,
e.g. `~/Library/Caches/readerkit/v1/oda-reader/1.10.0` on macOS. Resolution order:

1. `set_cache_dir()` (programmatic override)
1. `ODA_READER_CACHE_DIR` (environment variable)
1. `BBLOCKS_CACHE_DIR` (family-wide fallback shared across bblocks packages)
1. Platform default: `platformdirs.user_cache_dir("readerkit", appauthor=False)`

Whichever root wins, `readerkit` appends `v<schema>/oda-reader/<oda_reader version>`
beneath it, three extra path segments you don't choose.

### Get current cache location

```python
from oda_reader import get_cache_dir

location = get_cache_dir()
print(f"Cache directory: {location}")
```

### Change cache location

You can set a custom cache location:

```python
from oda_reader import set_cache_dir

# Move cache under your project directory
set_cache_dir("/path/to/my/project/oda_cache")
```

Files don't land directly in `oda_cache/`. `readerkit` nests them three levels
beneath whatever root you pass, so the actual cache directory becomes something
like `/path/to/my/project/oda_cache/v1/oda-reader/1.10.0/`.

Or use an environment variable (set before importing oda_reader):

```bash
export ODA_READER_CACHE_DIR="/path/to/cache"
```

```python
import oda_reader
# Cache is now at /path/to/cache/v1/oda-reader/1.10.0
```

### Reset to default location

```python
from oda_reader import reset_cache_dir

reset_cache_dir()
```

This reverts to the platform default described above.

## Managing the cache

### Clear all cached data

```python
from oda_reader import clear_cache

clear_cache()
```

This removes everything the cache stores: API responses, DataFrames, and bulk files. Your next query will hit the API again.

**When to clear cache**:

- You need the latest data and suspect OECD has updated
- Cache has grown too large
- You're troubleshooting unexpected results

**Using `oda_reader` alongside `oda_data`?** `clear_cache`, `set_cache_dir`,
`enable_cache` and `disable_cache` are deprecated under the umbrella package
and emit a `DeprecationWarning` pointing at the `oda_data.cache.*` API
(e.g. `oda_data.cache.clear("all")`). Standalone `oda_reader` users see no
warning. The shims continue to work through the `1.x` series and will be
removed in `2.0`.

### Enforcing size and age limits

`enforce_cache_limits()` deletes files older than a max age, then, if the
directory is still over a max size, deletes the oldest remaining files
until it's back under the limit:

```python
from oda_reader import enforce_cache_limits

enforce_cache_limits(max_size_mb=2500, max_age_hours=168)  # 2.5 GB, 7 days
```

Nothing calls this for you. Call it yourself when you want limits enforced.
It walks only the current version's cache directory, so space from
earlier versions stays on disk.

### Bulk file cache

The bulk file cache (used by `bulk_download_crs`, `download_crs_file`,
`bulk_download_dac1`, `bulk_download_dac2a`, `bulk_download_dac2b`, and `bulk_download_multisystem`)
is governed separately
because the files are large (up to 1.18 GB each):

- **Expiry**: every bulk file you download stays cached until its TTL
  below runs out, so disk use grows with the number of distinct files you
  fetch. Expired files are deleted the first time a Python session makes a
  bulk download call.
- **Publication-version invalidation**: a cached file is refetched as soon
  as OECD republishes it, not just after the TTL below expires. See
  [Cache invalidation on republish](#cache-invalidation-on-republish).
- **Per-entry TTL**: 90 days. A file older than that is refetched even if
  OECD hasn't republished it, which covers the cases where the
  publication-version check above couldn't be made (offline, server error,
  or the relevant headers absent).
- **Integrity validation**: every freshly downloaded payload is end-to-end
  checked before being trusted (a zip CRC walk, or a footer-magic check for
  the bare-parquet CRS files). Truncation is also caught earlier, during the
  download itself: streamed bytes are compared against the response's
  `Content-Length` and a mismatch is retried before it ever reaches the
  cache. A corrupt download is removed from the cache and raises
  `BulkPayloadCorruptError` so you can simply retry. Cached files are
  trusted on hit (no recheck on every call).
- **Self-healing**: temp files left behind by interrupted downloads (older
  than 24 hours) are swept on startup, so an aborted download can't pollute
  the cache directory indefinitely.
- **Atomic writes**: both the raw cache entry and any file written via
  `save_to_path` are written to a temporary sibling file and moved into
  place afterward, so a failed or interrupted write never leaves a
  truncated file where a good one used to be.

#### Cache invalidation on republish

OECD's bulk-download URLs are stable (`CRS.parquet` is always the same
address), so the
URL alone can't tell ODA Reader that OECD has replaced the file behind it.
Left unaddressed, a republish could sit unnoticed until the 90-day TTL
expired, and a pipeline could run against figures up to three months old
with nothing to indicate it.

The cache tracks a publication-version token alongside the URL, and a
change in that token invalidates the entry immediately, regardless of how
recently it was fetched. The token comes from the file host:

- **For every bulk file**, ODA Reader makes a one-byte ranged GET against
  the resolved URL and uses the server's `ETag` (falling back to
  `Last-Modified` if no `ETag` is sent) as the token. Any rewrite of the
  file changes it, including one where the version stamp in the SDMX
  annotation label, such as `CRS-Parquet-v20260803`, stays the same.
- If that request fails (offline, a server error, or a response carrying
  neither header), the cache keeps the token it stored and invalidation
  falls back to the 90-day TTL rather than failing the call. A cached file
  still works offline. A blocked request (for example a Cloudflare
  challenge) logs a warning naming the HTTP status or Cloudflare Ray ID.

You don't need to call anything for this. It runs automatically on every
bulk download.

#### Bypassing the bulk file cache

If you need a fresh download every call (e.g. for a CI job that should always
hit the source), pass `use_raw_cache=False`:

```python
from oda_reader import bulk_download_crs

# Download to a temp directory and discard the payload after extraction
crs = bulk_download_crs(use_raw_cache=False)
```

Validation still runs in this mode. Only the on-disk caching is skipped. The
flag is available on `bulk_download_crs`, `download_crs_file`,
`bulk_download_dac1`, `bulk_download_dac2a`, `bulk_download_dac2b`, and
`bulk_download_multisystem`. `download_aiddata` takes a different code path and is not affected.

#### Stale dataflow metadata

ODA Reader resolves a bulk file's download URL from the SDMX dataflow's
metadata, and that metadata response is itself subject to the 7-day HTTP
cache described under [HTTP caching](#http-caching-separate-from-dataframe-cache). The URLs themselves are stable (`CRS.parquet` is
always the same address), so a routine OECD republish doesn't go stale
here. The version-token invalidation in
[Cache invalidation on republish](#cache-invalidation-on-republish) handles that.

What this guards against is the URL itself changing, e.g. if OECD retires
the specific dataflow version a cached metadata response was fetched
under. If a download gets back a 404, ODA Reader bypasses the 7-day
cache for that one metadata lookup, re-resolves the URL against the live
OECD server, and retries the download once with the fresh result. This is
separate from the bulk-file cache above (which governs the downloaded
parquet/zip itself) and from `clear_version_cache()` (which governs
discovered dataflow _versions_, not resolved file URLs). You don't need to
call anything to get this behaviour. It runs automatically for
`bulk_download_crs()`, `download_crs_file()`, `bulk_download_dac1()`,
`bulk_download_dac2a()`, `bulk_download_dac2b()`, and `bulk_download_multisystem()`.

#### Handling corrupt downloads

The OECD's bulk endpoint occasionally serves a truncated or malformed file.
When that happens, a `BulkPayloadCorruptError` is raised and the bad entry is
already removed from disk by the time you see it, so the next call cleanly
re-downloads:

```python
from oda_reader import bulk_download_crs, BulkPayloadCorruptError

try:
    crs = bulk_download_crs()
except BulkPayloadCorruptError:
    # Bad entry already removed — just retry
    crs = bulk_download_crs()
```

## HTTP caching (separate from DataFrame cache)

ODA Reader also caches raw HTTP responses using `requests-cache`. HTTP responses expire after 7 days.

```python
from oda_reader import (
    enable_http_cache,
    disable_http_cache,
    clear_http_cache,
    get_http_cache_info
)
```

### Enable/disable HTTP cache

```python
# HTTP cache is enabled by default

# Disable temporarily
disable_http_cache()

# Re-enable
enable_http_cache()
```

### Clear version discovery cache

ODA Reader caches discovered dataflow versions in-process. If the OECD publishes a new version mid-session:

```python
from oda_reader import clear_version_cache

clear_version_cache()
```

This forces a fresh metadata lookup on the next query.

### Clear HTTP cache only

```python
# Clear just HTTP cache (keeps DataFrame cache)
clear_http_cache()
```

### Get HTTP cache info

```python
info = get_http_cache_info()
print(f"HTTP cache: {info['response_count']} responses cached")
```

**Difference between caches**:

- **HTTP cache**: Raw API responses (before parsing)
- **DataFrame cache**: Processed DataFrames (after schema translation)

Both caches speed up repeated queries, but DataFrame cache is faster since it skips parsing.

## Rate limiting

To avoid hitting OECD's API rate limits, ODA Reader automatically pauses between requests.

**Default rate limit**: 20 calls per 60 seconds

This is conservative and should prevent rate limit errors. You can customize it:

```python
from oda_reader import API_RATE_LIMITER

# More aggressive (use carefully)
API_RATE_LIMITER.max_calls = 30
API_RATE_LIMITER.period = 60  # seconds

# More conservative (if you're getting rate limit errors)
API_RATE_LIMITER.max_calls = 10
API_RATE_LIMITER.period = 60
```

**How rate limiting works**:

1. ODA Reader tracks each API call timestamp
1. Before a new call, it checks if limit is reached
1. If limit reached, it **blocks** (pauses) until period expires
1. Then allows the call to proceed

This is transparent. Your code just runs slower when rate limit is reached.

**Example**: If you make 20 calls in 30 seconds, the 21st call waits 30 more seconds before proceeding.

## Performance tips

### Use bulk downloads for large queries

If you need large amounts of data, bulk downloads are faster than API calls:

```python
from oda_reader import bulk_download_crs

# Much faster than download_crs() for full dataset
crs_full = bulk_download_crs()
```

See [Bulk Downloads](bulk-downloads.md) for details.

### Cache survives across sessions

Once data is cached, it stays cached between Python sessions:

```python
# Session 1
from oda_reader import download_dac1
data = download_dac1(start_year=2022, end_year=2022)  # slow

# Session 2 (later, even after restarting Python)
from oda_reader import download_dac1
data = download_dac1(start_year=2022, end_year=2022)  # instant
```

Cache persists until you clear it. HTTP responses expire after 7 days and bulk files after 30.

### Filter aggressively to reduce API load

Smaller queries are faster and more cache-friendly:

```python
# Slow: downloads everything
data = download_crs(start_year=2010, end_year=2023)

# Faster: filter for what you need
data = download_crs(
    start_year=2010,
    end_year=2023,
    filters={"donor": "USA", "sector": "120"}
)
```
