# Bulk Downloads

For large-scale analysis, bulk downloads are faster and more reliable than repeated API calls. ODA Reader provides bulk download functions for CRS, Multisystem, and AidData datasets.

## When to Use Bulk Downloads

**Use bulk downloads when**:
- You need the full CRS dataset (millions of rows)
- You're analyzing large year ranges
- You want all columns and dimensions
- API queries are too slow or hitting rate limits
- You need reproducible research with exact dataset versions

**Use API downloads when**:
- You need filtered subsets (specific donors, recipients, sectors)
- Working with smaller datasets (DAC1, DAC2a)
- Exploratory analysis with changing queries
- You only need recent data

## CRS Bulk Downloads

The full Creditor Reporting System dataset is available as a parquet file (~1GB compressed). ODA Reader can download and load it for you.

### Download Full CRS

```python
from oda_reader import bulk_download_crs

# Download and return as DataFrame (loads ~1GB into memory)
crs_data = bulk_download_crs()

print(f"Downloaded {len(crs_data)} rows")
print(f"Memory usage: {crs_data.memory_usage(deep=True).sum() / 1024**2:.1f} MB")
```

**Warning**: The full CRS is large. Loading it entirely into memory requires several GB of RAM.

### Save to Disk Instead

To avoid memory issues, save directly to disk:

```python
# Save to a folder instead of loading into memory
bulk_download_crs(save_to_path="./data/crs_full.parquet")
```

Then load it separately with pandas when needed:

```python
import pandas as pd

# Load the saved file
crs_data = pd.read_parquet("./data/crs_full.parquet")
```

### Reduced Version (Smaller File)

OECD provides a "reduced" version with fewer columns:

```python
# Download reduced version (smaller file, fewer columns)
crs_reduced = bulk_download_crs(reduced_version=True)
```

Or save to disk:

```python
bulk_download_crs(
    save_to_path="./data/crs_reduced.parquet",
    reduced_version=True
)
```

The reduced version omits some descriptive columns but retains all flow amounts and key dimensions.

## Memory-Efficient Processing with Iterators

For very large files, process in chunks to avoid loading the entire dataset into memory:

```python
# Process in chunks (much lower memory usage)
for chunk in bulk_download_crs(as_iterator=True):
    # chunk is a DataFrame with a subset of rows

    # Filter or aggregate each chunk
    filtered = chunk[chunk['donor_code'] == 'USA']

    # Save results or accumulate statistics
    filtered.to_csv("usa_projects.csv", mode="a", header=False)
```

**How it works**: `as_iterator=True` yields one DataFrame per parquet row group (typically 10,000-100,000 rows). You process each chunk sequentially, which keeps memory usage low.

**Example use cases**:
- Filtering large files: Process each chunk, save matches
- Computing aggregates: Accumulate statistics across chunks
- Converting formats: Read parquet chunks, write to CSV/Excel

**Combining with filtering**:

```python
# Filter for education sector projects while streaming
education_count = 0
education_amount = 0

for chunk in bulk_download_crs(as_iterator=True):
    education = chunk[chunk['purpose_code'].str.startswith('11')]
    education_count += len(education)
    education_amount += education['usd_commitment'].sum()

print(f"Education projects: {education_count}")
print(f"Total commitments: ${education_amount/1e9:.1f}B")
```

## Year-Specific CRS Files

OECD also provides individual files for specific years:

```python
from oda_reader import download_crs_file

# Download 2022 CRS data only
crs_2022 = download_crs_file(year=2022)

# Or save to disk
download_crs_file(year=2022, save_to_path="./data/crs_2022.parquet")
```

**Grouped years**: Older years are grouped in single files:
- Recent years: Individual files (2006-present)
- `"2004-05"`: 2004-2005 combined
- `"2002-03"`: 2002-2003 combined
- `"2000-01"`: 2000-2001 combined
- `"1995-99"`: 1995-1999 combined
- `"1973-94"`: 1973-1994 combined

**Example**:

```python
# Download historical data
crs_90s = download_crs_file(year="1995-99")
```

Year-specific files are much smaller than the full CRS, making them easier to work with.

## Multisystem Bulk Download

The full Multisystem dataset is available as a single parquet file:

```python
from oda_reader import bulk_download_multisystem

# Download and return as DataFrame
multisystem_data = bulk_download_multisystem()

# Or save to disk
bulk_download_multisystem(save_to_path="./data/multisystem.parquet")
```

Multisystem is smaller than CRS, so memory is less of a concern. However, iterators are still supported:

```python
# Process Multisystem in chunks
for chunk in bulk_download_multisystem(as_iterator=True):
    # Process each chunk
    pass
```

## AidData Download

AidData (Chinese development finance) comes from an Excel file automatically downloaded and parsed:

```python
from oda_reader import download_aiddata

# Download full AidData dataset
aiddata = download_aiddata()

# Filter by commitment year
aiddata_recent = download_aiddata(start_year=2015, end_year=2020)

# Save to disk
download_aiddata(save_to_path="./data/aiddata.parquet")
```

**Note**: AidData filtering happens after download (Excel file is downloaded first, then filtered). It's not querying an API like the DAC datasets.

## Important: Bulk Files Use .Stat Schema

**Critical difference**: Bulk download files from OECD use the **OECD.Stat schema**, not the Data Explorer API schema.

This means:
- Column names differ from API downloads
- Dimension codes may differ
- No `pre_process` or `dotstat_codes` parameters (files are already in .Stat format)

**Example**:

API download has columns like:
- `DONOR` → becomes `donor_code` after processing
- `RECIPIENT` → becomes `recipient_code` after processing

Bulk downloads already have:
- `DonorCode`
- `RecipientCode`

See [Schema Translation](schema-translation.md) for detailed comparison.

## Combining Bulk and API Downloads

You can mix approaches:

```python
# Download full CRS as bulk file
crs_full = bulk_download_crs()

# Use API for recent updates or specific queries
crs_recent = download_crs(
    start_year=2023,
    end_year=2023,
    filters={"donor": "USA"}
)

# Combine if schemas match
# (you may need to harmonize column names first)
```

## Performance Comparison

Approximate times (varies by network speed and OECD server load):

| Method | Dataset Size | Time |
|--------|-------------|------|
| API download (filtered) | 10,000 rows | 10-30 seconds |
| API download (large query) | 100,000 rows | 2-5 minutes |
| Bulk download CRS | ~2 million rows | 1-2 minutes |
| Bulk + iterator (filter) | Process 2 million rows | 2-5 minutes |

Bulk downloads are consistently fast regardless of query complexity, while API times vary significantly with query size.

## Troubleshooting

**Out of memory errors**: Use `as_iterator=True` to process in chunks instead of loading the entire file.

**Slow download**: Bulk downloads depend on OECD's file server speed. Try again later if slow. Once downloaded, files are cached.

**Column names don't match examples**: You're likely comparing bulk downloads (.Stat schema) to API downloads. See [Schema Translation](schema-translation.md).

**File not found errors**: Older CRS year-specific files use grouped years (e.g., "1995-99"). Check which grouping includes your target year.

## Next Steps

- **[Caching & Performance](caching.md)** - Understand how bulk downloads are cached
- **[Schema Translation](schema-translation.md)** - Learn about .Stat vs. API schema differences
- **[Filtering Data](filtering.md)** - Apply filters to bulk downloaded data using pandas
