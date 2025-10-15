# Cache Management in oda_reader

This document provides a comprehensive guide to the caching system in `oda_reader`, including architecture details, usage instructions, and backward compatibility notes.

---

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Quick Start](#quick-start)
4. [Configuration](#configuration)
5. [Cache Management](#cache-management)
6. [Backward Compatibility](#backward-compatibility)
7. [Performance Considerations](#performance-considerations)
8. [Troubleshooting](#troubleshooting)
9. [Technical Details](#technical-details)

---

## Overview

The `oda_reader` package uses a **three-tier caching system** to optimize data downloads from the OECD API:

1. **HTTP Cache** (requests-cache): Caches raw API responses for 7 days
2. **DataFrame Cache**: Caches processed DataFrames with preprocessing parameters
3. **Bulk File Cache**: Caches large bulk downloads (CRS, Multisystem, AidData)

This multi-layer approach provides:
- **Fast repeated queries** (10-90x speedup on cache hits)
- **Correct data** (cache keys include all processing parameters)
- **Efficient storage** (parquet format, automatic cleanup)
- **Platform-aware paths** (follows OS conventions)

---

## Architecture

```
User Request
     ↓
┌─────────────────────────────────────────┐
│  DataFrame Cache                        │
│  - Parquet files                        │
│  - Includes preprocessing params        │
│  - Key: hash(url + pre_process + ...)  │
└─────────────────────────────────────────┘
     ↓ (cache miss)
┌─────────────────────────────────────────┐
│  HTTP Cache (requests-cache)            │
│  - SQLite backend                       │
│  - 7-day TTL                            │
│  - Caches 200 and 404 responses         │
└─────────────────────────────────────────┘
     ↓ (cache miss)
┌─────────────────────────────────────────┐
│  OECD API                               │
│  - Rate limited (20 calls/60s)          │
└─────────────────────────────────────────┘
```

### Cache Directory Structure

Default location: `~/.cache/oda-reader/{version}/` (macOS/Linux) or `%LOCALAPPDATA%\oda-reader\Cache\{version}` (Windows)

```
~/.cache/oda-reader/1.2.2/
├── http_cache.sqlite          # HTTP response cache
├── dataframes/                # Processed DataFrames
│   ├── 2986243275235237.parquet
│   └── 00b396b02a62f1cb.parquet
└── bulk_files/                # Bulk downloads
    ├── manifest.json
    ├── .cache.lock
    └── 6545c6d5a9c7d8a.zip
```

**Note**: Cache is automatically versioned - upgrading `oda_reader` creates a new cache directory, ensuring compatibility.

---

## Quick Start

### Basic Usage (No Configuration Required)

Caching is **enabled by default** and works automatically:

```python
import oda_reader

# First call: fetches from API (~2s)
df = oda_reader.download_dac1(start_year=2022, end_year=2022)

# Second call: loads from cache (~0.02s) - 100x faster!
df = oda_reader.download_dac1(start_year=2022, end_year=2022)
```

### Check Cache Status

```python
import oda_reader

# HTTP cache stats
print(oda_reader.get_http_cache_info())
# {'response_count': 8, 'redirects_count': 0}

# DataFrame cache stats
print(oda_reader.dataframe_cache().stats())
# {'total_entries': 2, 'total_size_mb': 0.37}

# Bulk file cache stats
print(oda_reader.bulk_cache_manager().stats())
# {'total_entries': 1, 'total_size_mb': 878.5, 'stale_entries': 0}
```

---

## Configuration

### Cache Directory

By default, cache is stored in a platform-specific location. You can customize this:

```python
import oda_reader

# Get current cache directory
print(oda_reader.get_cache_dir())
# /Users/jorge/Library/Caches/oda-reader/1.2.2

# Set custom cache directory
oda_reader.set_cache_dir("/custom/path/to/cache")

# Reset to default
oda_reader.reset_cache_dir()
```

**Environment Variable**: You can also set `ODA_READER_CACHE_DIR`:

```bash
export ODA_READER_CACHE_DIR="/custom/cache/path"
```

Priority order:
1. `set_cache_dir()` (programmatic override)
2. `ODA_READER_CACHE_DIR` (environment variable)
3. Platform default (via platformdirs)

### Disable Caching

For testing or when you need fresh data:

```python
import oda_reader

# Disable all caching
oda_reader.disable_cache()

# Disable only HTTP caching
oda_reader.disable_http_cache()

# Disable only DataFrame caching
oda_reader.dataframe_cache().disable()

# Re-enable
oda_reader.enable_cache()
```

### Rate Limiting

API rate limiting is independent of caching:

```python
import oda_reader

# Default: 20 calls per 60 seconds
oda_reader.API_RATE_LIMITER.max_calls = 10
oda_reader.API_RATE_LIMITER.period = 60
```

---

## Cache Management

### Clearing Cache

```python
import oda_reader

# Clear all cache (entire directory)
oda_reader.clear_cache()

# Clear only HTTP cache
oda_reader.clear_http_cache()

# Clear only DataFrame cache
oda_reader.dataframe_cache().clear()

# Clear only bulk files
oda_reader.bulk_cache_manager().clear()

# Clear specific bulk file
oda_reader.bulk_cache_manager().clear("crs_full")
```

### Inspecting Cache

```python
import oda_reader

# List all bulk file cache entries
for record in oda_reader.bulk_cache_manager().list_records():
    print(f"{record['key']}: {record['size_mb']:.1f} MB, "
          f"age: {record['age_days']:.1f} days, "
          f"stale: {record['is_stale']}")
```

### Manual Cache Enforcement (Advanced)

```python
import oda_reader

# Enforce cache limits (max size, max age)
oda_reader.enforce_cache_limits(
    max_size_mb=2500,  # 2.5 GB
    max_age_hours=168  # 7 days
)
```

**Note**: This is called automatically on first cache access, not at import time.

---

## Backward Compatibility

### Legacy API (Still Supported)

All old functions continue to work:

```python
import oda_reader

# Old API (still works)
oda_reader.cache_dir()           #  Returns cache directory
oda_reader.enable_cache()        #  Enables all caching
oda_reader.disable_cache()       #  Disables all caching
oda_reader.clear_cache()         #  Clears entire cache
oda_reader.enforce_cache_limits()  #  Enforces size/age limits
```

### Migration Guide

If you have code using the old caching system:

**Before** (oda_reader < 1.2.2):
```python
from oda_reader._cache import memory, set_cache_dir

# Old joblib-based caching
mem = memory()
if mem.store_backend:
    print("Cache enabled")

set_cache_dir("/custom/path")
```

**After** (oda_reader >= 1.2.2):
```python
from oda_reader import get_cache_dir, set_cache_dir

# New requests-cache + DataFrame cache
print(f"Cache at: {get_cache_dir()}")
set_cache_dir("/custom/path")
```

**Key Changes**:
- ✅ No breaking changes - old code continues to work
- ✅ Cache location moved from `src/oda_reader/.cache/` to platform directory
- ✅ joblib replaced with requests-cache + parquet files
- ✅ Cache keys now include preprocessing parameters (fixes correctness issue)
- ✅ No import-time side effects (was: `enforce_cache_limits()` ran on import)

### Why the Refactor?

The old caching system had several issues:

1. **Data correctness bug**: Cache didn't include `pre_process` or `dotstat_codes` parameters
   ```python
   # Before: These returned the SAME cached data (wrong!)
   df1 = download_dac1(2022, 2022, pre_process=True, dotstat_codes=True)
   df2 = download_dac1(2022, 2022, pre_process=False, dotstat_codes=False)

   # After: These correctly return different data
   ```

2. **Bad cache location**: Cache was in `src/oda_reader/.cache/` (polluted source tree)

3. **Import-time slowdown**: `enforce_cache_limits()` walked entire cache on every import

4. **No observability**: No way to inspect cache contents or hit/miss rates

All these issues are now fixed while maintaining full backward compatibility.

---

## Performance Considerations

### Cache Hit Performance

Typical speedups with cache hits:
- **HTTP cache hit**: ~2-5x faster (avoids network request)
- **DataFrame cache hit**: ~10-90x faster (avoids parsing + processing)

Example benchmark:
```
First download:  2.71s (API + processing)
Second download: 0.03s (DataFrame cache) - 90x faster
```

### Storage Usage

Typical cache sizes:
- HTTP cache: 1-10 MB (SQLite database)
- DataFrame cache: 0.1-1 MB per query (compressed parquet)
- Bulk files: 100-1000 MB per file (CRS full dataset ~900 MB)

### Cache Expiration

- **HTTP cache**: 7 days (604800 seconds)
- **DataFrame cache**: No automatic expiration (cleared manually or on size limit)
- **Bulk files**: Configurable TTL (default: 30 days for CRS, 180 days for AidData)

### When Cache Is NOT Used

Cache is bypassed when:
1. Caching is disabled (`disable_cache()` or `disable_http_cache()`)
2. Cache entry has expired (HTTP: 7 days, bulk files: per-entry TTL)
3. Different parameters are used (cache keys are unique per parameter combination)

---

## Troubleshooting

### Cache Not Working

**Check if caching is enabled:**
```python
import oda_reader

# This should show cached responses after first download
print(oda_reader.get_http_cache_info())
print(oda_reader.dataframe_cache().stats())
```

**Common issues:**
- Caching disabled: Call `oda_reader.enable_cache()`
- Cache full: Call `oda_reader.clear_cache()` or increase limits
- Different parameters: Cache keys are unique per parameter combination

### Cache Growing Too Large

**Check cache size:**
```python
import oda_reader

# Check overall cache size
from oda_reader._cache import get_cache_size_mb
print(f"Cache size: {get_cache_size_mb():.1f} MB")

# Check individual caches
print(oda_reader.dataframe_cache().stats())
print(oda_reader.bulk_cache_manager().stats())
```

**Solutions:**
```python
# Clear specific caches
oda_reader.dataframe_cache().clear()  # Usually the culprit
oda_reader.clear_http_cache()

# Or clear everything
oda_reader.clear_cache()

# Adjust limits
oda_reader.enforce_cache_limits(max_size_mb=1000)  # 1 GB limit
```

### Cache Returning Stale Data

**Force fresh data:**
```python
# Option 1: Clear cache before download
oda_reader.clear_http_cache()

# Option 2: Temporarily disable cache
oda_reader.disable_cache()
# download
oda_reader.enable_cache()
```

### Permission Errors

If you get permission errors accessing the cache:

```python
import oda_reader

# Set cache to a writable location
oda_reader.set_cache_dir("/tmp/oda_cache")
```

### Multi-Process Issues

The bulk file cache uses `FileLock` for multi-process safety. If you see lock timeout errors:

```python
from oda_reader._cache import CacheManager

# Increase lock timeout (default: 1200s)
manager = CacheManager()
manager._lock = FileLock(manager.lock_path, timeout=2000)
```

---

## Technical Details

### Module Structure

```
oda_reader/_cache/
├── __init__.py       # Public API exports
├── config.py         # Cache directory configuration
├── manager.py        # Bulk file cache (pydeflate-style)
├── dataframe.py      # DataFrame caching layer
└── legacy.py         # Backward-compatible functions
```

### Cache Key Generation

**DataFrame cache keys** are SHA256 hashes of:
```python
{
    "dataflow_id": "DSD_DAC1@DF_DAC1",
    "dataflow_version": "1.6",
    "url": "https://...",
    "pre_process": True,
    "dotstat_codes": True,
    # ... any other parameters
}
```

This ensures different preprocessing options get separate cache entries.

**Bulk file cache keys** are simple strings like `"crs_full"`, `"aiddata"`.

### HTTP Cache Backend

Uses `requests-cache` with SQLite backend:
- Database: `{cache_dir}/http_cache.sqlite`
- Stores responses, redirects, and metadata
- Automatic cleanup on expiration
- Thread-safe for concurrent requests

### DataFrame Cache Format

- Format: Apache Parquet (compressed, column-oriented)
- Location: `{cache_dir}/dataframes/{cache_key}.parquet`
- Compression: Snappy (default)
- Typical compression ratio: 5-10x

### Bulk File Cache

Follows pydeflate design:
- Manifest: `{cache_dir}/bulk_files/manifest.json`
- Lock file: `{cache_dir}/bulk_files/.cache.lock` (FileLock)
- Atomic writes: temp-file-then-rename pattern
- Metadata: download timestamp, version, TTL, size

### Version-Based Cache Invalidation

Cache directory includes package version:
```
~/.cache/oda-reader/1.2.2/  # Version 1.2.2
~/.cache/oda-reader/1.3.0/  # Version 1.3.0 (new cache)
```

This ensures:
- No cache corruption after upgrades
- Schema changes don't break existing cache
- Automatic cleanup of old versions

---

## API Reference

### Configuration Functions

- `get_cache_dir() -> Path`: Get current cache directory
- `set_cache_dir(path: str | Path) -> None`: Set custom cache directory
- `reset_cache_dir() -> None`: Reset to platform default

### HTTP Cache Functions

- `enable_http_cache() -> None`: Enable HTTP caching
- `disable_http_cache() -> None`: Disable HTTP caching
- `clear_http_cache() -> None`: Clear all HTTP responses
- `get_http_cache_info() -> dict`: Get cache statistics

### DataFrame Cache

- `dataframe_cache() -> DataFrameCache`: Get DataFrame cache instance
- `DataFrameCache.stats() -> dict`: Get statistics
- `DataFrameCache.clear() -> None`: Clear all cached DataFrames
- `DataFrameCache.enable() -> None`: Enable caching
- `DataFrameCache.disable() -> None`: Disable caching

### Bulk File Cache

- `bulk_cache_manager() -> CacheManager`: Get bulk cache instance
- `CacheManager.stats() -> dict`: Get statistics
- `CacheManager.list_records() -> list[dict]`: List all cached files
- `CacheManager.clear(key: str | None) -> None`: Clear cache entries

### Legacy Functions (Backward Compatibility)

- `cache_dir() -> Path`: Alias for `get_cache_dir()`
- `enable_cache() -> None`: Enable all caching
- `disable_cache() -> None`: Disable all caching
- `clear_cache() -> None`: Clear entire cache directory
- `enforce_cache_limits() -> None`: Enforce size/age limits

---
