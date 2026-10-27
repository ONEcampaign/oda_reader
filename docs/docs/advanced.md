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

This filter string can be used to manually construct API URLs. Standalone, `QueryBuilder` orders the key by the dimension order of each table's latest data structure at release time. The `download_*` functions pass the order of the version they resolve, so the same filters give the right key for any version. To follow a specific order yourself, pass `dimensions=("DONOR", "SECTOR", ...)` to the constructor.

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

Without `dataflow_version`, ODA Reader reads the latest published version of the dataflow. The OECD keeps superseded versions online with their data frozen, so a pinned old version still answers with a 200 and silently lacks the newest year.

### Automatic version discovery

The first query for a dataflow in a process looks up its metadata, with the data structure, from the OECD SDMX endpoint. The response names the latest version and lists that version's key dimensions in order. ODA Reader logs the version it reads at INFO level, for example `Reading dataflow DSD_DAC1@DF_DAC1 version 1.8.`

ODA Reader repeats the latest-version lookup after a day, and the HTTP cache keeps a latest-version response for one day. A new release therefore shows up within two days of publication, including in a notebook or server that runs for days. An explicit version is looked up once per process.

ODA Reader builds the query key from that structure. It maps each filter to its dimension ID (`DONOR`, `SECTOR`, `MEASURE`, and so on) and places the value at the position the structure gives that ID. Versions that reorder dimensions need no code change. DSD_DAC1 1.5 and 1.8 have the same seven dimensions in a different order, and each gets a correct key.

A filter and a version's dimensions can disagree in two ways:

- A filter on a dimension the version lacks raises a `ValueError` naming the dimension, the dataflow and the version.
- A dimension that ODA Reader has no filter for is read unfiltered, and a warning names it. An unfiltered dimension can carry a total code, so sums over the result may double-count.

If the metadata request fails and nothing is cached, ODA Reader raises a `ConnectionError`.

### Clearing the version cache

Resolved versions are cached in-process so repeated queries don't trigger extra metadata lookups. To see a dataflow version the OECD published within the last two days, force a fresh lookup:

```python
from oda_reader import clear_version_cache

clear_version_cache()
```

The next lookup of each dataflow after the call skips the HTTP cache and reaches the OECD, including for a dataflow the process has not looked up yet. Later lookups use the caches as usual.

### Manual version override

Pin an exact dataflow version with `dataflow_version`:

```python
from oda_reader import download_dac1

data = download_dac1(
    start_year=2022,
    end_year=2022,
    dataflow_version="1.5"
)
```

A pinned version gets the same treatment as the latest. ODA Reader reads that version's data structure and builds the key from it.

**When to override**:

- You need the same version for reproducibility
- You are debugging version-specific issues

If the OECD does not publish the pinned version, a `ConnectionError` says the dataflow has no such version and that omitting `dataflow_version` reads the latest.

**Available for**:

- `download_dac1(dataflow_version=...)`
- `download_dac2a(dataflow_version=...)`
- `download_dac2b(dataflow_version=...)`
- `download_crs(dataflow_version=...)`
- `download_multisystem(dataflow_version=...)`
- `download_cpa(dataflow_version=...)`

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

### Enable logging

ODA Reader logs through loggers under the name `oda_reader` and leaves handlers and levels to your
application. Importing the package does not configure logging, so by default you see only warnings
and errors.

To see the INFO messages, such as downloads and cache hits, configure logging in your application:

```python
import logging

logging.basicConfig(level=logging.INFO)
```

To raise only ODA Reader, give your application a handler and set the level on the `oda_reader`
logger:

```python
import logging

logging.basicConfig(level=logging.WARNING)
logging.getLogger("oda_reader").setLevel(logging.INFO)
```

To silence everything below errors:

```python
import logging

logging.getLogger("oda_reader").setLevel(logging.ERROR)
```

The `oda_importer` logger name is deprecated and will be removed in 2.0. A level or handler set on
it still applies to the package's messages, with a `DeprecationWarning`. A `dictConfig` or
`fileConfig` that names `oda_importer` silences the package's loggers, so name `oda_reader` there.

### Inspect caches

Check where the caches live and clear them to force fresh downloads:

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
