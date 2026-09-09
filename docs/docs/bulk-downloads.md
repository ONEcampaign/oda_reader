# Bulk Downloads

For large-scale analysis, bulk downloads are faster and more reliable than repeated API calls. ODA Reader provides bulk download functions for the CRS, DAC1, DAC2a, DAC2b, Multisystem, and AidData datasets.

## When to Use Bulk Downloads

**Use bulk downloads when**:

- You need the full CRS dataset (millions of rows)
- You're analyzing large year ranges
- You want all columns and dimensions
- API queries are too slow or hitting rate limits
- You need reproducible research with exact dataset versions

**Use API downloads when**:

- You need filtered subsets (specific donors, recipients, sectors)
- Working with smaller datasets (DAC1, DAC2a, DAC2b)
- Exploratory analysis with changing queries
- You only need recent data

## CRS Bulk Downloads

The full Creditor Reporting System dataset is available as a bare parquet file (1.18 GB, not zipped). ODA Reader can download and load it for you.

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

OECD provides a "reduced" version with fewer columns (296 MB, also a bare parquet file):

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

## Memory-Efficient Processing with Iterators

For very large files, process in chunks to avoid loading the entire dataset into memory. This
section covers `bulk_download_crs()` and `bulk_download_multisystem()`, which stream parquet row
groups directly off disk. `download_crs_file()`'s year-specific files are a different case, with
a memory caveat of their own; see [Processing a Year-Specific File in Chunks](#processing-a-year-specific-file-in-chunks) below.

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

## Forcing a Fresh Download

By default, bulk downloads are cached on disk so a second call returns
instantly. You don't need `use_raw_cache=False` just to pick up an OECD
republish; the cache detects that on its own and refetches (see
[Cache Invalidation on Republish](caching.md#cache-invalidation-on-republish)).
Reach for `use_raw_cache=False` when you want to skip the cache outright, for
example in a CI job that should always hit the network:

```python
# Always download fresh; the payload is written to a temp dir and discarded
crs = bulk_download_crs(use_raw_cache=False)
```

The integrity check on the freshly downloaded payload still runs; only the
on-disk caching is skipped. This flag is available on `bulk_download_crs`,
`download_crs_file`, `bulk_download_dac1`, `bulk_download_dac2a`,
`bulk_download_dac2b`, and `bulk_download_multisystem`.

See [Caching & Performance](caching.md#bulk-file-cache) for how the bulk
cache is managed (LRU eviction, TTL, integrity validation).

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

### Processing a Year-Specific File in Chunks

`download_crs_file(year, as_iterator=True)` also works, yielding one `DataFrame` per 100,000 rows:

```python
from oda_reader import download_crs_file

chunks = list(download_crs_file(year=2024, as_iterator=True))
print(len(chunks))                  # 4
print(sum(len(c) for c in chunks))  # 370072, same row count as the non-iterator call
```

!!! warning "Heads up"
This isn't a streaming parse. The year files are pipe-delimited text, not
parquet, so `as_iterator=True` reads the whole file first and hands it
back in 100,000-row slices. Peak memory during the read is the same as
the non-iterator call; only what you accumulate chunk-by-chunk
afterward is bounded. Row-group streaming that does reduce peak memory
during the read itself is the bare-parquet path used by
`bulk_download_crs()` (see
[Memory-Efficient Processing with Iterators](#memory-efficient-processing-with-iterators)
above).

!!! info "Why"
Reading the file whole and slicing, rather than parsing it in chunks
directly, sidesteps a real bug: pandas infers each chunk's dtypes
independently. On the 2024 CRS data, `Interest1` mixes numeric codes
like `"04216"` with formulas like `"EURIBOR6M+1.60%"`. A chunk that
happens to contain only numeric-looking rows gets parsed as float and
silently drops the leading zero. Reading once and slicing guarantees
every chunk matches what the non-iterator call would have returned.

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

## DAC1 Bulk Download

OECD publishes the full DAC1 table as one zipped CSV, 18.7 MB compressed and 1,042,278 rows:

```python
from oda_reader import bulk_download_dac1

# Download and return as DataFrame
dac1_data = bulk_download_dac1()

# Or save to disk
bulk_download_dac1(save_to_path="./data/")
```

Passing `save_to_path` converts the CSV to parquet and writes `table1_data.parquet` (the source
filename, lowercased) into the folder you pass. Calling without it returns a DataFrame directly.

DAC1's 14 columns pair a code with a label for each dimension: `DONOR`/`Donor`, `PART`/`Part`,
`AIDTYPE`/`Aid type`, `FLOWS`/`Fund flows`, `AMOUNTTYPE`/`Amount type`, `TIME`/`Year`, plus
`Value` and `Flags`. Three of the label columns contain spaces, so access them by string index:
`dac1_data["Aid type"]`. See
[`bulk_download_dac1()` Uses Paired Code and Label Columns](#bulk_download_dac1-uses-paired-code-and-label-columns)
below for the full comparison against the other bulk files.

`Flags` is empty throughout the current release and reads as `float64` `NaN`. Treat its dtype as
unstable across OECD republishes. Populated values would arrive as `object`.

OECD publishes DAC1 as a full dataset only. There is no year-specific file, unlike CRS.

Iterators are supported:

```python
from oda_reader import bulk_download_dac1

for chunk in bulk_download_dac1(as_iterator=True):
    # process each chunk here
    ...
```

!!! warning "Heads up"
DAC1 is a zipped CSV, so `as_iterator=True` leaves peak memory unchanged. It reads the whole
file first and hands it back in slices, the same profile as
`download_crs_file(as_iterator=True)`. See the caveat under
[Processing a Year-Specific File in Chunks](#processing-a-year-specific-file-in-chunks). Only
what you accumulate chunk-by-chunk afterward is bounded.

## DAC2b Bulk Download

OECD publishes the full DAC2b table as one zipped CSV, 20.6 MB compressed and 2,281,210 rows,
expanding to 459.8 MB:

```python
from oda_reader import bulk_download_dac2b

# Download and return as DataFrame
dac2b_data = bulk_download_dac2b()

# Or save to disk
bulk_download_dac2b(save_to_path="./data/")
```

Passing `save_to_path` converts the CSV to parquet and writes `table2b_data.parquet` (the source
filename, lowercased) into the folder you pass. Calling without it returns a DataFrame directly.
Either way, the conversion reads the full 459.8 MB CSV into memory before writing parquet, so
`save_to_path` reduces what you retain afterward, not the peak memory the conversion itself uses.

DAC2b's 14 columns pair a code with a label for each dimension, the same legacy .Stat layout DAC1
uses: `RECIPIENT`/`Recipient`, `DONOR`/`Donor`, `PART`/`Part`, `AIDTYPE`/`Aid type`,
`DATATYPE`/`Amount type`, `TIME`/`Year`, plus `Value` and `Flags`. This differs from the 26-column
layout `download_dac2b()` returns from the API. The bulk file carries a `PART` dimension (Part I
developing countries / Part II) that the API dataflow does not expose. Its `AIDTYPE` codes are the
legacy .Stat aid types (e.g. `204` "Other Long-term - Amounts Extended"); on the default path
(`dotstat_codes=True`), `download_dac2b()`'s `aidtype_code` column carries the same numbering, so
the two match directly (the API's own `MEASURE` codes, e.g. `2204`, only show up with
`dotstat_codes=False` — see the [DAC2b page](datasets.md#dac2b-other-official-flows-and-export-credits)
for both numberings). See
[`bulk_download_dac2b()` Carries a `PART` Dimension the API Doesn't Expose](#bulk_download_dac2b-carries-a-part-dimension-the-api-doesnt-expose)
below for the full comparison.

`Flags` is empty throughout the current release and reads as `float64` `NaN`, the same
unstable-dtype caveat DAC1's `Flags` column carries.

OECD publishes DAC2b as a full dataset only. There is no year-specific file, unlike CRS.

Iterators are supported:

```python
from oda_reader import bulk_download_dac2b

for chunk in bulk_download_dac2b(as_iterator=True):
    # process each chunk here
    ...
```

!!! warning "Heads up"
DAC2b is a zipped CSV, so `as_iterator=True` leaves peak memory unchanged, the same profile as
`bulk_download_dac1(as_iterator=True)`. See the caveat under
[Processing a Year-Specific File in Chunks](#processing-a-year-specific-file-in-chunks). Only
what you accumulate chunk-by-chunk afterward is bounded.

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

Bulk downloads already have `DonorCode`, `RecipientCode`, and similar PascalCase names, with two exceptions, covered next.

### `bulk_download_crs()` Uses Different Column Casing Than Everything Else

`CRS.parquet` and `CRS-reduced.parquet` (what `bulk_download_crs()` downloads) are bare parquet
files, not zips, and OECD ships their columns in snake_case: `donor_code`, `donor_name`,
`crs_id`. The year-specific CRS files from `download_crs_file()` and Multisystem are zips and use
PascalCase: `DonorCode`, `DonorName`, `CrsID`. DAC1, DAC2a, and DAC2b pair a code column with a
label column instead, covered next.

```python
from oda_reader import bulk_download_crs, download_crs_file

full_crs = bulk_download_crs()
print(list(full_crs.columns)[:5])
# ['year', 'donor_code', 'de_donorcode', 'donor_name', 'agency_code']

year_crs = download_crs_file(year=2022)
print(list(year_crs.columns)[:5])
# ['Year', 'DonorCode', 'DEDonorcode', 'DonorName', 'AgencyCode']
```

!!! warning "Heads up"
Both calls return the same underlying data with the same .Stat codes, just
named differently. Code written against `bulk_download_crs()` that filters
on `donor_code` will raise a `KeyError` if you point it at
`download_crs_file()` output instead, and vice versa with `DonorCode`.
Check `df.columns` after switching between the two.

See [Schema Translation](schema-translation.md) for detailed comparison.

### `bulk_download_dac1()` Uses Paired Code and Label Columns

`Table1_Data.zip` (what `bulk_download_dac1()` downloads) pairs a code column with a label
column for each dimension: `DONOR`/`Donor`, `PART`/`Part`, `AIDTYPE`/`Aid type`,
`FLOWS`/`Fund flows`, `AMOUNTTYPE`/`Amount type`, `TIME`/`Year`, plus `Value` and `Flags`. None
of these are PascalCase, and three of the label columns contain spaces, so access them by
string index:

```python
from oda_reader import bulk_download_dac1

dac1_data = bulk_download_dac1()
dac1_data["Aid type"]
```

`Table2a_Data.zip` and `Table2b_Data.zip` (`bulk_download_dac2a()` and `bulk_download_dac2b()`)
share this same paired-column shape, unlike the year-specific CRS files and Multisystem above. See
[`bulk_download_dac2b()` Carries a `PART` Dimension the API Doesn't Expose](#bulk_download_dac2b-carries-a-part-dimension-the-api-doesnt-expose)
below for DAC2b's full column list, and [DAC1 Bulk Download](#dac1-bulk-download) above for
DAC1's.

### `bulk_download_dac2b()` Carries a `PART` Dimension the API Doesn't Expose

`Table2b_Data.zip` (what `bulk_download_dac2b()` downloads) pairs a code column with a label
column for each dimension, the same shape as DAC1's and DAC2a's bulk files: `RECIPIENT`/`Recipient`,
`DONOR`/`Donor`, `PART`/`Part`, `AIDTYPE`/`Aid type`, `DATATYPE`/`Amount type`, `TIME`/`Year`, plus
`Value` and `Flags`.

One of those columns has no equivalent in the API dataflow, and one uses a numbering that only
matches the API on one of its two paths:

- `PART` (Part I developing countries / Part II) has no equivalent dimension in the API dataflow.
- `AIDTYPE` carries legacy .Stat aid-type codes (e.g. `204` "Other Long-term - Amounts Extended").
  The API's own numbering is `MEASURE` (e.g. `2204` "OOF loans, disbursements"), but on the
  default path (`dotstat_codes=True`) `download_dac2b()` converts its `aidtype_code` column to
  the same .Stat numbering as `AIDTYPE`, so a bulk/API join on (`aidtype_code`, `recipient_code`)
  = (`AIDTYPE`, `RECIPIENT`) needs no translation — confirmed by joining a live USA 2022 pull
  against `Table2b_Data.zip` this way: every row matched, with a maximum absolute value
  difference of 0.0. Only `dotstat_codes=False` returns the raw `MEASURE` codes, which do differ
  from `AIDTYPE`.

```python
from oda_reader import bulk_download_dac2b

dac2b_data = bulk_download_dac2b()
dac2b_data["Aid type"]
```

See [DAC2b Bulk Download](#dac2b-bulk-download) above for the full column list and usage details.

## Troubleshooting

**Out of memory errors**: `as_iterator=True` reduces peak memory on `bulk_download_crs()` and `bulk_download_multisystem()`, which stream parquet row groups. On `download_crs_file()`'s year-specific files and `bulk_download_dac1()` it reads the whole file first and only bounds what you accumulate afterward. See the caveat under [Processing a Year-Specific File in Chunks](#processing-a-year-specific-file-in-chunks).

**Slow download**: Bulk downloads depend on OECD's file server speed. Try again later if slow. Once downloaded, files are cached. Each attempt has a 10-second connect / 60-second read timeout, so a stalled connection now fails with an error instead of hanging indefinitely.

### Cloudflare challenge (`BulkDownloadChallengeError`)

Cloudflare can require an interactive browser check on an OECD bulk-file
request. These checks require browser execution. ODA Reader raises
`BulkDownloadChallengeError` and includes the `cf_ray` value supplied by
OECD/Cloudflare. Include that value when reporting the failure to OECD.

When bulk access is unavailable, choose a narrowly filtered API request
explicitly.

```python
from oda_reader import download_dac1

dac1_2024 = download_dac1(
    start_year=2024,
    end_year=2024,
    filters={"donor": "USA"},
)
```

```python
from oda_reader import download_crs

crs_2024 = download_crs(
    start_year=2024,
    end_year=2024,
    filters={"donor": "USA", "recipient": "KEN"},
)
```

Keep API requests bounded by time period and filters. Bulk files and API
responses use different schemas. The DAC1 API omits the bulk-file `PART`
dimension.

**Column names don't match examples**: You're likely comparing bulk downloads (.Stat schema) to API downloads, or comparing `bulk_download_crs()` output to `download_crs_file()` output. See the column-casing difference above, and [Schema Translation](schema-translation.md) for the API-vs-bulk comparison.

**File not found / 404 errors**: Bulk-download URLs are stable, so a routine OECD republish doesn't cause this (see [Caching & Performance](caching.md#cache-invalidation-on-republish) for how a republish is detected instead). A 404 here usually means the specific dataflow version was retired; ODA Reader re-resolves the URL and retries once automatically when that happens. If it still fails, older CRS year-specific files use grouped years (e.g., "1995-99"); check which grouping includes your target year.

**`BulkPayloadCorruptError`**: The OECD's bulk endpoint occasionally serves a
truncated or malformed file (a bad zip, or a bare parquet file that fails to
parse). The corrupt entry is removed automatically before the exception is
raised, so the next call cleanly re-downloads. Retry the call, or pass
`use_raw_cache=False` to skip the cache for that invocation.

## Next Steps

- **[Caching & Performance](caching.md)** - Understand how bulk downloads are cached
- **[Schema Translation](schema-translation.md)** - Learn about .Stat vs. API schema differences
- **[Filtering Data](filtering.md)** - Apply filters to bulk downloaded data using pandas
