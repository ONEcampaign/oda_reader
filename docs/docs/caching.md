# Caching & Performance

ODA Reader uses caching to make repeated queries fast and reduce dependency on OECD's servers. This page explains how caching works and how to configure it.

## How Caching Works

ODA Reader caches three types of data:

1. **HTTP responses**: Raw API responses before processing
2. **DataFrames**: Processed pandas DataFrames after schema translation
3. **Bulk files**: Large parquet/zip files downloaded by `bulk_download_crs`,
   `download_crs_file`, `bulk_download_dac2a` and `bulk_download_multisystem`

All three caches are automatic and transparent - you don't need to change your code to benefit from caching.

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

## Cache Location

By default, caches are stored in:
```
src/oda_reader/.cache/
```

This is inside the package installation directory.

### Get Current Cache Location

```python
from oda_reader import get_cache_dir

location = get_cache_dir()
print(f"Cache directory: {location}")
```

### Change Cache Location

You can set a custom cache location:

```python
from oda_reader import set_cache_dir

# Move cache to your project directory
set_cache_dir("/path/to/my/project/oda_cache")
```

Or use an environment variable (set before importing oda_reader):

```bash
export ODA_READER_CACHE_DIR="/path/to/cache"
```

```python
import oda_reader
# Cache is now at /path/to/cache
```

### Reset to Default Location

```python
from oda_reader import reset_cache_dir

reset_cache_dir()
```

This reverts to the default location inside the package directory.

## Managing the Cache

### Clear All Cached Data

```python
from oda_reader import clear_cache

clear_cache()
```

This removes all cached API responses and DataFrames. Your next query will hit the API again.

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

### Automatic Cache Cleanup

ODA Reader automatically enforces cache limits across the cache root:

- **Max size**: 2.5 GB
- **Max age**: 7 days

When you import oda_reader, it checks cache limits:
- Files older than 7 days are deleted
- If cache exceeds 2.5 GB, oldest files are deleted first

This happens automatically - you don't need to do anything.

### Bulk File Cache

The bulk file cache (used by `bulk_download_crs`, `download_crs_file`,
`bulk_download_dac2a` and `bulk_download_multisystem`) is governed separately
because the files are large (~1 GB each):

- **LRU eviction**: only the two most recent bulk files are kept; older
  entries are removed automatically the next time you import oda_reader.
- **Per-entry TTL**: an entry is considered stale after 30 days and refetched
  on next use.
- **Integrity validation**: every freshly downloaded zip is end-to-end checked
  before being trusted. A corrupt download is removed from the cache and
  raises `BulkPayloadCorruptError` so you can simply retry. Cached files are
  trusted on hit (no recheck on every call).
- **Self-healing**: temp files left behind by interrupted downloads (older
  than 24 hours) are swept on startup, so an aborted download can't pollute
  the cache directory indefinitely.

#### Bypassing the Bulk File Cache

If you need a fresh download every call (e.g. for a CI job that should always
hit the source), pass `use_raw_cache=False`:

```python
from oda_reader import bulk_download_crs

# Download to a temp directory and discard the zip after extraction
crs = bulk_download_crs(use_raw_cache=False)
```

Validation still runs in this mode; only the on-disk caching is skipped. The
flag is available on `bulk_download_crs`, `download_crs_file`,
`bulk_download_dac2a` and `bulk_download_multisystem`. `download_aiddata`
takes a different code path and is not affected.

#### Handling Corrupt Downloads

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

## HTTP Caching (Separate from DataFrame Cache)

ODA Reader also caches raw HTTP responses using `requests-cache`:

```python
from oda_reader import (
    enable_http_cache,
    disable_http_cache,
    clear_http_cache,
    get_http_cache_info
)
```

### Enable/Disable HTTP Cache

```python
# HTTP cache is enabled by default

# Disable temporarily
disable_http_cache()

# Re-enable
enable_http_cache()
```

### Clear Version Discovery Cache

ODA Reader caches discovered dataflow versions in-process. If the OECD publishes a new version mid-session:

```python
from oda_reader import clear_version_cache

clear_version_cache()
```

This forces a fresh metadata lookup on the next query.

### Clear HTTP Cache Only

```python
# Clear just HTTP cache (keeps DataFrame cache)
clear_http_cache()
```

### Get HTTP Cache Info

```python
info = get_http_cache_info()
print(f"HTTP cache: {info['cache_size']} responses cached")
```

**Difference between caches**:
- **HTTP cache**: Raw API responses (before parsing)
- **DataFrame cache**: Processed DataFrames (after schema translation)

Both caches speed up repeated queries, but DataFrame cache is faster since it skips parsing.

## Rate Limiting

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
2. Before a new call, it checks if limit is reached
3. If limit reached, it **blocks** (pauses) until period expires
4. Then allows the call to proceed

This is transparent - your code just runs slower when rate limit is reached.

**Example**: If you make 20 calls in 30 seconds, the 21st call waits 30 more seconds before proceeding.

## Performance Tips

### Use Bulk Downloads for Large Queries

If you need large amounts of data, bulk downloads are faster than API calls:

```python
from oda_reader import bulk_download_crs

# Much faster than download_crs() for full dataset
crs_full = bulk_download_crs()
```

See [Bulk Downloads](bulk-downloads.md) for details.

### Cache Survives Across Sessions

Once data is cached, it stays cached between Python sessions:

```python
# Session 1
from oda_reader import download_dac1
data = download_dac1(start_year=2022, end_year=2022)  # slow

# Session 2 (later, even after restarting Python)
from oda_reader import download_dac1
data = download_dac1(start_year=2022, end_year=2022)  # instant
```

Cache persists until you clear it or it expires (7 days).

### Filter Aggressively to Reduce API Load

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
