---
description: Build SDMX filters with QueryBuilder, pin dataflow versions, tune rate limits, combine queries, and debug failed downloads.
---

# Advanced Topics

## Using QueryBuilder directly

`QueryBuilder` is the internal class that constructs SDMX API queries. You can use it directly for custom queries:

```python
from oda_reader import QueryBuilder

qb = QueryBuilder(dataflow_id="DSD_DAC1@DF_DAC1")

# Build a custom DAC1 filter
filter_string = qb.build_dac1_filter(
    donor="USA",
    sector="_Z",
    measure="1010",
    flow_type="1140"
)

print(filter_string)
# Output: USA._Z.1010..1140..
```

This filter string can be used to manually construct API URLs.

**When to use QueryBuilder directly**:

- Building custom SDMX queries
- Debugging filter construction
- Understanding dimension order for a dataset

**Methods available**:

- `build_dac1_filter(donor, sector, measure, flow_type, unit_measure, price_base)`
- `build_dac2_filter(donor, recipient, measure, price_base, ...)` serves both DAC2a and DAC2b. The old `build_dac2a_filter` name still works and is deprecated
- `build_crs_filter(donor, recipient, sector, channel, modality, microdata, ...)`
- `build_multisystem_filter(donor, channel, flow_type, ...)`

Each method returns a filter string suitable for the SDMX API.

## Dataflow version handling

OECD occasionally changes dataflow versions (schema updates). ODA Reader handles this automatically with version discovery.

### Automatic version discovery

When a dataflow version returns an error (not found), ODA Reader automatically queries the OECD's SDMX metadata endpoint to discover the latest published version and retries with it:

1. Tries the configured default version
1. If not found, queries the metadata endpoint for the authoritative latest version
1. Retries once with the discovered version

If the new version changes the number of key dimensions, ODA Reader raises a `ConnectionError` and asks you to upgrade `oda_reader`. Otherwise it continues with the discovered version, and you see a log message indicating which version was discovered.

### Clearing the version cache

Discovered versions are cached in-process so repeated queries don't trigger extra metadata lookups. If the OECD publishes a new dataflow version mid-session, you can force re-discovery:

```python
from oda_reader import clear_version_cache

clear_version_cache()
```

### Manual version override

You can specify an exact dataflow version:

```python
from oda_reader import download_dac1

data = download_dac1(
    start_year=2022,
    end_year=2022,
    dataflow_version="1.7"
)
```

**When to override**:

- You know the correct version for reproducibility
- Debugging version-specific issues

**Available for**:

- `download_dac1(dataflow_version=...)`
- `download_dac2a(dataflow_version=...)`
- `download_dac2b(dataflow_version=...)`
- `download_crs(dataflow_version=...)`
- `download_multisystem(dataflow_version=...)`

## API version differences

OECD uses two SDMX API versions:

**API v1** (legacy):

```
https://sdmx.oecd.org/public/rest/data/OECD.DCD.FSD,DSD_DAC1@DF_DAC1,1.8/...
```

**API v2** (current):

```
https://sdmx.oecd.org/public/rest/v2/data/dataflow/OECD.DCD.FSD/DSD_DAC1@DF_DAC1/1.8/...
```

ODA Reader uses the appropriate version for each dataset:

- **DAC1, DAC2a, DAC2b**: API v1
- **CRS, CPA, Multisystem**: Custom endpoint (CRS-specific API)

You generally don't need to worry about this - ODA Reader handles it automatically.

## Custom rate limiting strategies

Beyond basic rate limiting configuration, you can implement custom strategies:

### Disable rate limiting (use carefully)

```python
from oda_reader import API_RATE_LIMITER

# Effectively disable (very high limit)
API_RATE_LIMITER.max_calls = 1000
API_RATE_LIMITER.period = 1
```

**Warning**: This may get you blocked by OECD's servers. Only use for testing or if you have permission.

### Dynamic rate limiting

```python
# Start conservative
API_RATE_LIMITER.max_calls = 10
API_RATE_LIMITER.period = 60

# Make some calls...

# Adjust based on response times or errors
experiencing_slowdowns = True  # set from your own monitoring
if experiencing_slowdowns:
    API_RATE_LIMITER.max_calls = 5  # Slow down
```

### Check rate limiter state

```python
# Access internal state (undocumented, subject to change)
print(f"Calls made: {len(API_RATE_LIMITER._calls)}")
print(f"Max calls: {API_RATE_LIMITER.max_calls}")
```

## Combining multiple queries

For complex analysis, you might combine multiple queries:

```python
from oda_reader import download_dac1, download_dac2a
import pandas as pd

# Get donor totals from DAC1
donor_totals = download_dac1(start_year=2022, end_year=2022)

# Get bilateral flows from DAC2a
bilateral = download_dac2a(start_year=2022, end_year=2022)

# Merge for analysis
# (Note: ensure compatible schemas and codes)
combined = pd.merge(
    donor_totals,
    bilateral,
    on=['donor_code', 'year'],
    how='inner'
)
```

**Tips**:

- Use the same `pre_process` and `dotstat_codes` settings (see [Schema Translation](schema-translation.md))
- Column names and codes must align
- Filter carefully to avoid double-counting

### Load schema mapping files

```python
import json
from pathlib import Path

# Find ODA Reader installation
import oda_reader
package_path = Path(oda_reader.__file__).parent

# Load the donor code map (.Stat code to API code)
with open(package_path / 'schemas/mappings/dac1_codes_area.json') as f:
    mapping = json.load(f)

# Use for custom translation
print(dict(list(mapping.items())[:3]))
# {'1': 'AUT', '2': 'BEL', '3': 'DNK'}
```

## Working with large datasets in production

For production pipelines with large datasets:

### Use bulk downloads and local storage

```python
from oda_reader import bulk_download_crs
import pandas as pd

# Download once, save locally
bulk_download_crs(save_to_path="/data/")

# In your pipeline, read from local file (fast)
def get_crs_data():
    return pd.read_parquet("/data/CRS.parquet")
```

## Debugging tips

### Inspect caches

ODA Reader doesn't have built-in verbose logging yet, but you can inspect behaviour:

```python
# Check cache behavior
from oda_reader import get_cache_dir, get_http_cache_info

print(f"Cache location: {get_cache_dir()}")
print(f"HTTP cache info: {get_http_cache_info()}")

# Clear caches to force fresh downloads
from oda_reader import clear_cache, clear_http_cache
clear_cache()
clear_http_cache()
```

### Test with small queries first

```python
from oda_reader import download_crs

# Test with single year, single donor
test_data = download_crs(
    start_year=2022,
    end_year=2022,
    filters={"donor": "USA"}
)

print(f"Columns: {list(test_data.columns)}")
print(f"Shape: {test_data.shape}")
print(test_data.head())
```

### Compare API and bulk schema

```python
from oda_reader import bulk_download_crs, download_crs

# Download same data both ways
api_data = download_crs(
    start_year=2022,
    end_year=2022,
    filters={"donor": "USA"}
)

bulk_data = bulk_download_crs()
bulk_filtered = bulk_data[
    (bulk_data['year'] == 2022) &
    (bulk_data['donor_code'].astype(str) == '302')  # 302 is the .Stat code for USA
]

print("API columns:", list(api_data.columns)[:10])
print("Bulk columns:", list(bulk_filtered.columns)[:10])
```

## Next steps

- **[Getting Started](getting-started.md)** - Return to basics if needed
- **[GitHub Issues](https://github.com/ONEcampaign/oda_reader/issues)** - Report bugs or request features
