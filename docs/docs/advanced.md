# Advanced Topics

This page covers advanced features and customization options for power users.

## Using QueryBuilder Directly

`QueryBuilder` is the internal class that constructs SDMX API queries. You can use it directly for custom queries:

```python
from oda_reader import QueryBuilder

qb = QueryBuilder()

# Build a custom DAC1 filter
filter_string = qb.build_dac1_filter(
    donor="USA",
    measure="1010",
    flow_type="1140"
)

print(filter_string)
# Output: "USA.....1010.1140..."
```

This filter string can be used to manually construct API URLs.

**When to use QueryBuilder directly**:
- Building custom SDMX queries
- Debugging filter construction
- Understanding dimension order for a dataset

**Methods available**:
- `build_dac1_filter(donor, recipient, flow_type, measure, unit_measure, price_base)`
- `build_dac2a_filter(donor, recipient, measure, price_base, ...)`
- `build_crs_filter(donor, recipient, sector, channel, modality, microdata, ...)`
- `build_multisystem_filter(donor, channel, flow_type, ...)`

Each method returns a filter string suitable for the SDMX API.

## Dataflow Version Handling

OECD occasionally changes dataflow versions (schema updates). ODA Reader handles this automatically with version fallback.

### Automatic Fallback

When a dataflow version returns 404 (not found), ODA Reader automatically:

1. Tries the configured version (e.g., `1.5`)
2. If 404, retries with `1.4`
3. Continues decrementing: `1.3`, `1.2`, `1.1`
4. Returns data from first successful version (up to 5 attempts)

This means your code keeps working even when OECD makes breaking schema changes.

**Example**:

```python
from oda_reader import download_dac1

# ODA Reader will automatically try:
# 1.5 -> 404
# 1.4 -> 404
# 1.3 -> Success! Returns data with version 1.3
data = download_dac1(start_year=2022, end_year=2022)
```

You'll see a message indicating which version succeeded.

### Manual Version Override

You can specify an exact dataflow version:

```python
# Force use of version 1.3
data = download_dac1(
    start_year=2022,
    end_year=2022,
    dataflow_version="1.3"
)
```

**When to override**:
- You know the correct version for reproducibility
- Debugging version-specific issues
- Avoiding automatic fallback (for performance)

**Available for**:
- `download_dac1(dataflow_version=...)`
- `download_dac2a(dataflow_version=...)`
- `download_crs(dataflow_version=...)`
- `download_multisystem(dataflow_version=...)`

## API Version Differences

OECD uses two SDMX API versions:

**API v1** (legacy):
```
https://sdmx.oecd.org/public/rest/data/OECD.DCD.FSD,DF_DAC1,1.0/...
```

**API v2** (current):
```
https://sdmx.oecd.org/public/rest/v2/data/dataflow/OECD.DCD.FSD/DF_DAC1/1.0/...
```

ODA Reader uses the appropriate version for each dataset:
- **DAC1, DAC2a**: API v2
- **CRS, Multisystem**: Custom endpoint (CRS-specific API)

You generally don't need to worry about this - ODA Reader handles it automatically.

## Custom Rate Limiting Strategies

Beyond basic rate limiting configuration, you can implement custom strategies:

### Disable Rate Limiting (Use Carefully)

```python
from oda_reader import API_RATE_LIMITER

# Effectively disable (very high limit)
API_RATE_LIMITER.max_calls = 1000
API_RATE_LIMITER.period = 1
```

**Warning**: This may get you blocked by OECD's servers. Only use for testing or if you have permission.

### Dynamic Rate Limiting

```python
# Start conservative
API_RATE_LIMITER.max_calls = 10
API_RATE_LIMITER.period = 60

# Make some calls...

# Adjust based on response times or errors
if experiencing_slowdowns:
    API_RATE_LIMITER.max_calls = 5  # Slow down
```

### Check Rate Limiter State

```python
# Access internal state (undocumented, subject to change)
print(f"Calls made: {len(API_RATE_LIMITER.call_times)}")
print(f"Max calls: {API_RATE_LIMITER.max_calls}")
```

## Combining Multiple Queries

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
    on=['donor', 'year'],
    how='inner'
)
```

**Tips**:
- Use same `pre_process` and `dotstat_codes` settings for compatibility
- Column names and codes must align
- Filter carefully to avoid double-counting

### Load Schema Mapping Files

```python
import json
from pathlib import Path

# Find ODA Reader installation
import oda_reader
package_path = Path(oda_reader.__file__).parent

# Load schema mapping
with open(package_path / 'schemas/mappings/dac1_mapping.json') as f:
    mapping = json.load(f)

# Use for custom translation
print(mapping['DONOR']['mapping'])
# {'1': 'AUS', '2': 'AUT', ...}
```

## Working with Large Datasets in Production

For production pipelines with large datasets:

### Use Bulk Downloads + Local Storage

```python
from oda_reader import bulk_download_crs
import pandas as pd

# Download once, save locally
bulk_download_crs(save_to_path="/data/crs_full.parquet")

# In your pipeline, read from local file (fast)
def get_crs_data():
    return pd.read_parquet("/data/crs_full.parquet")
```

## Debugging Tips

### Enable Verbose Logging

ODA Reader doesn't have built-in verbose logging yet, but you can inspect behavior:

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

### Test with Small Queries First

```python
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

### Compare API vs. Bulk Schema

```python
# Download same data both ways
api_data = download_crs(
    start_year=2022,
    end_year=2022,
    filters={"donor": "USA"}
)

bulk_data = bulk_download_crs()
bulk_filtered = bulk_data[
    (bulk_data['Year'] == 2022) &
    (bulk_data['DonorCode'] == 'USA')
]

print("API columns:", list(api_data.columns)[:10])
print("Bulk columns:", list(bulk_filtered.columns)[:10])
```

## Next Steps

- **[API Reference](api-reference.md)** - Complete function signatures and parameters
- **[Getting Started](getting-started.md)** - Return to basics if needed
- **GitHub Issues** - Report bugs or request features at the ODA Reader repository
