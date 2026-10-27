---
description: Download the full CRS, DAC1, DAC2b, Multisystem and AidData files, stream the CRS and Multisystem files in chunks to save memory, and handle their .Stat schema.
---

# Bulk Downloads

For large-scale analysis, bulk downloads are faster and more reliable than repeated API calls. ODA Reader provides bulk download functions for the CRS, DAC1, DAC2a, DAC2b, Multisystem, and AidData datasets.

## When to use bulk downloads

**Use bulk downloads when**:

- You need the full CRS dataset (millions of rows)
- You're analysing large year ranges
- You want all columns and dimensions
- API queries are too slow or hitting rate limits
- You need reproducible research with exact dataset versions

**Use API downloads when**:

- You need filtered subsets (specific donors, recipients, sectors)
- Working with smaller datasets (DAC1, DAC2a, DAC2b)
- Exploratory analysis with changing queries
- You only need recent data

## How ODA Reader fetches bulk files

OECD publishes the bulk files on `webfs-dcd.oecd.org`. ODA Reader downloads each file whole,
keeps a local copy in the [bulk file cache](caching.md#bulk-file-cache) and answers every query
from that copy. It never reads a bulk file remotely.

Before a download, ODA Reader sends one single-byte request to read the file's version token
(its `ETag`, or `Last-Modified` when no `ETag` is sent). It downloads the file only when it has
no copy, when the token has changed, or when the copy is past its 90-day TTL. Otherwise it reads
the copy it has. See [Cache invalidation on republish](caching.md#cache-invalidation-on-republish).

OECD may change bulk file names, for example by adding a timestamp. ODA Reader does not hard-code
them. On each online read it looks up the current file name in the SDMX dataflow annotations,
and an offline read finds the cached copy by dataset, whatever name it was stored under.

A scripted request can meet a Cloudflare challenge; see
[Cloudflare challenge](#cloudflare-challenge-bulkdownloadchallengeerror). The supported route
then is to download the file in a browser and pass it to `register_bulk_file`.

## CRS bulk downloads

The full Creditor Reporting System dataset is available as a bare parquet file (1.18 GB, not zipped). ODA Reader can download and load it for you.

### Download full CRS

```python
from oda_reader import bulk_download_crs

# Download and return as DataFrame (the file is 1.18 GB, see the warning below)
crs_data = bulk_download_crs()

print(f"Downloaded {len(crs_data)} rows")
print(f"Memory usage: {crs_data.memory_usage(deep=True).sum() / 1024**2:.1f} MB")
```

**Warning**: The full CRS is large. Loading it entirely into memory requires several GB of RAM.

### Save to disk instead

To avoid memory issues, save directly to disk:

```python
# Save to a folder instead of loading into memory (writes ./data/CRS.parquet)
bulk_download_crs(save_to_path="./data/")
```

Then load it separately with pandas when needed:

```python
import pandas as pd

# Load the saved file
crs_data = pd.read_parquet("./data/CRS.parquet")
```

### Reduced version (smaller file)

OECD provides a "reduced" version with fewer columns (296 MB, also a bare parquet file):

```python
# Download reduced version (smaller file, fewer columns)
crs_reduced = bulk_download_crs(reduced_version=True)
```

Or save to disk:

```python
bulk_download_crs(
    save_to_path="./data/",
    reduced_version=True
)
```

## Memory-efficient processing with iterators

For very large files, process in chunks to avoid loading the entire dataset into memory. This
section covers `bulk_download_crs()` and `bulk_download_multisystem()`, which stream parquet row
groups directly off disk. `download_crs_file()`'s year-specific files are a different case, with
a memory caveat of their own. See [Processing a year-specific file in chunks](#processing-a-year-specific-file-in-chunks) below.

```python
# Process in chunks (much lower memory usage)
for chunk in bulk_download_crs(as_iterator=True):
    # chunk is a DataFrame with a subset of rows

    # Filter or aggregate each chunk
    filtered = chunk[chunk['donor_code'].astype(str) == '302']  # 302 is the .Stat code for USA

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

## Forcing a fresh download

By default, bulk downloads are cached on disk so a second call returns
instantly. You don't need `use_raw_cache=False` just to pick up an OECD
republish. The cache detects that on its own and refetches (see
[Cache invalidation on republish](caching.md#cache-invalidation-on-republish)).
Reach for `use_raw_cache=False` when you want to skip the cache outright, for
example in a CI job that should always hit the network:

```python
# Always download fresh; the payload is written to a temp dir and discarded
crs = bulk_download_crs(use_raw_cache=False)
```

The integrity check on the freshly downloaded payload still runs. Only the
on-disk caching is skipped. This flag is available on `bulk_download_crs`,
`download_crs_file`, `bulk_download_dac1`, `bulk_download_dac2a`,
`bulk_download_dac2b`, and `bulk_download_multisystem`.

See [Caching & Performance](caching.md#bulk-file-cache) for how the bulk
cache is managed (TTL, expiry, integrity validation).

## Year-specific CRS files

OECD also provides individual files for specific years:

```python
from oda_reader import download_crs_file

# Download 2022 CRS data only
crs_2022 = download_crs_file(year=2022)

# Or save to disk (a folder)
download_crs_file(year=2022, save_to_path="./data/")
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

### Processing a year-specific file in chunks

`download_crs_file(year, as_iterator=True)` also works, yielding one `DataFrame` per 100,000 rows:

```python
from oda_reader import download_crs_file

chunks = list(download_crs_file(year=2024, as_iterator=True))
print(len(chunks))                  # 4
print(sum(len(c) for c in chunks))  # 370072, same row count as the non-iterator call
```

!!! warning
The year files are pipe-delimited text, so `as_iterator=True` reads the whole file first and hands it
back in 100,000-row slices. Peak memory during the read is the same as
the non-iterator call. Only what you accumulate chunk-by-chunk
afterward is bounded. Row-group streaming that does reduce peak memory
during the read itself is the bare-parquet path used by
`bulk_download_crs()` (see
[Memory-efficient processing with iterators](#memory-efficient-processing-with-iterators)
above).

!!! note
Reading the file whole and slicing, rather than parsing it in chunks
directly, sidesteps a real bug: pandas infers each chunk's dtypes
independently. On the 2024 CRS data, `Interest1` mixes numeric codes
like `"04216"` with formulas like `"EURIBOR6M+1.60%"`. A chunk that
happens to contain only numeric-looking rows gets parsed as float and
silently drops the leading zero. Reading once and slicing guarantees
every chunk matches what the non-iterator call would have returned.

## Multisystem bulk download

The full Multisystem dataset is available as a single parquet file:

```python
from oda_reader import bulk_download_multisystem

# Download and return as DataFrame
multisystem_data = bulk_download_multisystem()

# Or save to disk (a folder)
bulk_download_multisystem(save_to_path="./data/")
```

Multisystem is smaller than CRS, so memory is less of a concern. However, iterators are still supported:

```python
# Process Multisystem in chunks
for chunk in bulk_download_multisystem(as_iterator=True):
    # Process each chunk
    pass
```

## DAC1 bulk download

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
[`bulk_download_dac1()` uses paired code and label columns](#bulk_download_dac1-uses-paired-code-and-label-columns)
below for the full comparison against the other bulk files.

`Flags` is empty throughout the current release and reads as `float64` `NaN`. Treat its dtype as
unstable across OECD republishes. Populated values would arrive as `object`.

OECD publishes DAC1 as a full dataset only.

Iterators are supported:

```python
from oda_reader import bulk_download_dac1

for chunk in bulk_download_dac1(as_iterator=True):
    # process each chunk here
    ...
```

!!! warning
DAC1 is a zipped CSV, so `as_iterator=True` leaves peak memory unchanged. It reads the whole
file first and hands it back in slices, the same profile as
`download_crs_file(as_iterator=True)`. See the caveat under
[Processing a year-specific file in chunks](#processing-a-year-specific-file-in-chunks). Only
what you accumulate chunk-by-chunk afterward is bounded.

## DAC2b bulk download

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
`save_to_path` reduces what you retain afterward.

DAC2b's 14 columns pair a code with a label for each dimension, the same legacy .Stat layout DAC1
uses: `RECIPIENT`/`Recipient`, `DONOR`/`Donor`, `PART`/`Part`, `AIDTYPE`/`Aid type`,
`DATATYPE`/`Amount type`, `TIME`/`Year`, plus `Value` and `Flags`. This differs from the 26-column
layout `download_dac2b()` returns from the API. The bulk file carries a `PART` dimension (Part I
developing countries / Part II) that the API dataflow does not expose. Its `AIDTYPE` codes are the
legacy .Stat aid types (e.g. `204` "Other Long-term - Amounts Extended"). On the default path
(`dotstat_codes=True`), `download_dac2b()`'s `aidtype_code` column carries the same numbering, so
the two match directly (the API's own `MEASURE` codes, e.g. `2204`, only show up with
`dotstat_codes=False`). See the [DAC2b page](datasets.md#dac2b-other-official-flows-and-export-credits)
for both numberings. See
[`bulk_download_dac2b()` carries a `PART` dimension the API doesn't expose](#bulk_download_dac2b-carries-a-part-dimension-the-api-doesnt-expose)
below for the full comparison.

`Flags` is empty throughout the current release and reads as `float64` `NaN`, the same
unstable-dtype caveat DAC1's `Flags` column carries.

OECD publishes DAC2b as a full dataset only.

Iterators are supported:

```python
from oda_reader import bulk_download_dac2b

for chunk in bulk_download_dac2b(as_iterator=True):
    # process each chunk here
    ...
```

!!! warning
DAC2b is a zipped CSV, so `as_iterator=True` leaves peak memory unchanged, the same profile as
`bulk_download_dac1(as_iterator=True)`. See the caveat under
[Processing a year-specific file in chunks](#processing-a-year-specific-file-in-chunks). Only
what you accumulate chunk-by-chunk afterward is bounded.

## AidData download

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

**Note**: AidData filtering happens after download (Excel file is downloaded first, then filtered).

## Query bulk files in place with `scan()`

`scan()` returns a bulk dataset as one Parquet file on disk, which DuckDB, Polars or PyArrow can query without loading it into pandas. It covers `"dac1"`, `"dac2a"`, `"crs"` and `"multisystem"`:

```python
import duckdb
import oda_reader

dac2a = oda_reader.scan("dac2a")
dac2a.path      # PosixPath('.../bulk_files/<key>-<digest>.parquet')
dac2a.release   # '0x8DF2539E5D52F9D'

duckdb.sql(
    f"SELECT year, sum(value) FROM '{dac2a.path}' "
    "WHERE donor_code = 4 AND data_type_code = 'D' GROUP BY year"
)
```

`scan()` returns a frozen `ScannedTable` with five fields: `path`, `release`, `label_release`, `downloaded` (UTC) and `columns`. Read the file in place and leave it unmodified.

The returned path is named for its release, so its bytes stay the same after a later `scan()` downloads or converts a newer release. A DuckDB view built on it keeps reading the release it was built on. The files of the current and the previous release are kept, and older ones are deleted.

A reader that keeps a path for longer, through any number of newer releases, takes a lease. `oda_reader.lease(dataset)` scans as `scan()` does and returns a `TableLease`, whose `table` is the `ScannedTable`. No `scan()` deletes the file until the lease is closed:

```python
with oda_reader.lease("dac2a") as held:
    duckdb.sql(f"SELECT count(*) FROM '{held.table.path}'")
```

The lease protects the file only while the `TableLease` object is kept open. A lease dropped without `close()`, as in `oda_reader.lease("dac2a").table.path`, releases at once and leaves the path unprotected. Use a `with` block or hold the object for as long as the path is read.

A lease is a lock the operating system releases when its process exits, so a crashed reader leaves an empty, unlocked lease file that holds nothing, and the next prune of that release removes it.

CRS is published as Parquet, so `scan("crs")` returns a hard link to the cached bulk file, which takes no extra disk space. DAC1, DAC2A and Multisystem are published as zipped CSVs. The first `scan()` of a release streams the CSV into Parquet beside the cached zip, and later calls return that file until the release token changes. On a 2026 release the conversion took 0.6 s for DAC1 (1,046,691 rows), 6.3 s for DAC2A (10,115,276 rows, about 0.5 GB of memory) and 3.3 s for Multisystem (1,145,632 rows). Row groups hold 122,880 rows. The files are sorted by year, so a filter on year skips most groups. A filter on donor or recipient does not.

Converted columns are snake_case and unique case-insensitively. The raw `DONOR` and `Donor` collide in DuckDB, which is case-insensitive. They become `donor_code` and `donor_name`. The names match those `download_dac1()`, `download_dac2a()` and the CRS Parquet already use:

| Raw column (DAC1, DAC2A)                                | Converted                                           | Type             |
| ------------------------------------------------------- | --------------------------------------------------- | ---------------- |
| `DONOR` / `Donor`                                       | `donor_code` / `donor_name`                         | int32 / string   |
| `RECIPIENT` / `Recipient` (DAC2A)                       | `recipient_code` / `recipient_name`                 | int32 / string   |
| `PART` / `Part`                                         | `part_code` / `part_name`                           | int32 / string   |
| `AIDTYPE` / `Aid type`                                  | `aidtype_code` / `aid_type`                         | int32 / string   |
| `FLOWS` / `Fund flows` (DAC1)                           | `flows_code` / `fund_flows`                         | int32 / string   |
| `AMOUNTTYPE` (DAC1), `DATATYPE` (DAC2A) / `Amount type` | `amounttype_code`, `data_type_code` / `amount_type` | string           |
| `TIME`                                                  | `year`                                              | int32            |
| `Year`                                                  | dropped, it repeats `TIME`                          |                  |
| `Value` / `Flags`                                       | `value` / `flags`                                   | float64 / string |

Multisystem's CamelCase headers follow the CRS names: `Year` becomes `year`, `DonorCode` `donor_code`, `AgencyNameE` `agency_name`, `ChannelCode` `channel_code`, `RecipientCode` `recipient_code`, `Purposecode` `purpose_code`, `FlowType` `flow_type`, `AmountType` `amount_type`, `AidToOrThru` `aid_to_or_thru` and `Amount` `amount`. A column OECD adds later is kept as a string under its snake_case name.

### Release tokens

`oda_reader.release(dataset)` returns the token that names a published file. For every dataset it is the file host's ETag, else its Last-Modified date. Two copies of the same file carry the same token, whenever they were downloaded, and a file OECD rewrites gets a new one. A file cached without either, such as one registered while the host was unreachable, gets `"sha256:<digest of the file>"`. `scan()` reports the same token for the same cached file.

CRS also carries the readable id OECD appends to its dataflow annotation, such as `"v20260803"`. It is `ScannedTable.label_release`, kept apart from the token because OECD can rewrite the file without changing the label. It is `None` for DAC1, DAC2A and Multisystem, and for a CRS file whose host token could not be read.

Online, `release()` reads the token of the file OECD publishes now and downloads nothing. With `offline=True` it returns the cached copy's token.

### Options

`scan()` and `release()` take the same keyword arguments:

- `offline=True` makes no network request and uses the cached or registered copy, whatever its age. A cold cache raises `BulkFileNotCachedError`.
- `refresh=True` downloads the bulk file again and bypasses the cached dataflow annotations. It cannot be combined with `offline=True`.
- `cache_dir=` reads and writes the bulk file and its conversion under another cache root for this call only, leaving the process-wide root unchanged. The dataflow annotations are still looked up through the process-wide HTTP cache.

`oda_reader.cache_info()` lists the cached bulk files as `BulkFileInfo` records: `dataset`, `release`, `label_release`, `downloaded`, `size_bytes`, `derived_bytes`, `stale` and `path`. `derived_bytes` is the disk space of the Parquet files `scan()` made from the bulk file, and a CRS hard link counts as 0.

## Bulk files use the .Stat schema

**Critical difference**: Bulk download files from OECD use the **OECD.Stat schema**, not the Data Explorer API schema.

This means:

- Column names differ from API downloads
- Dimension codes may differ
- No `pre_process` or `dotstat_codes` parameters (files are already in .Stat format)

**Example**:

API download has columns like:

- `DONOR` → becomes `donor_code` after processing
- `RECIPIENT` → becomes `recipient_code` after processing

Year-specific CRS files already have `DonorCode`, `RecipientCode`, and similar PascalCase names. The full CRS parquet and the DAC tables differ, as covered next.

### `bulk_download_crs()` uses different column casing than everything else

`CRS.parquet` and `CRS-reduced.parquet` (what `bulk_download_crs()` downloads) are bare parquet
files, not zips, and OECD ships their columns in snake_case: `donor_code`, `donor_name`,
`crs_id`. The year-specific CRS files from `download_crs_file()` are zips and use
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

!!! warning
Both calls return the same underlying data with the same .Stat codes, just
named differently. Code written against `bulk_download_crs()` that filters
on `donor_code` will raise a `KeyError` if you point it at
`download_crs_file()` output instead, and vice versa with `DonorCode`.
Check `df.columns` after switching between the two.

See [Schema Translation](schema-translation.md) for detailed comparison.

### `bulk_download_dac1()` uses paired code and label columns

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
share this same paired-column shape. See
[`bulk_download_dac2b()` carries a `PART` dimension the API doesn't expose](#bulk_download_dac2b-carries-a-part-dimension-the-api-doesnt-expose)
below for DAC2b's full column list, and [DAC1 bulk download](#dac1-bulk-download) above for
DAC1's.

### `bulk_download_dac2b()` carries a `PART` dimension the API doesn't expose

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
  = (`AIDTYPE`, `RECIPIENT`) needs no translation. A join on those two columns matches every row of a USA 2022 pull
  against `Table2b_Data.zip`, with a maximum absolute value difference of 0.0. Only `dotstat_codes=False` returns the raw `MEASURE` codes, which do differ
  from `AIDTYPE`.

```python
from oda_reader import bulk_download_dac2b

dac2b_data = bulk_download_dac2b()
dac2b_data["Aid type"]
```

See [DAC2b bulk download](#dac2b-bulk-download) above for the full column list and usage details.

## Troubleshooting

**Out of memory errors**: `as_iterator=True` reduces peak memory on `bulk_download_crs()` and `bulk_download_multisystem()`, which stream parquet row groups. On `download_crs_file()`'s year-specific files, `bulk_download_dac1()`, and `bulk_download_dac2b()` it reads the whole file first and only bounds what you accumulate afterward. See the caveat under [Processing a year-specific file in chunks](#processing-a-year-specific-file-in-chunks).

**Slow download**: Bulk downloads depend on OECD's file server speed. Try again later if slow. Once downloaded, files are cached. Each attempt has a 10-second connect / 60-second read timeout, so a stalled connection fails with an error.

**HTTP 429 or 503**: When the file server answers with a `Retry-After` of 300 seconds or less, ODA Reader waits that long and tries again, up to three times. A longer wait, a missing `Retry-After`, or a fourth throttled response raises `BulkDownloadHTTPError`, and its `retry_after` attribute holds the wait the server asked for in seconds.

### Cloudflare challenge (`BulkDownloadChallengeError`)

OECD supports downloading the bulk files in a browser. Scripted downloads,
including the ones ODA Reader makes, are not a service OECD has committed
to, so OECD's file server can challenge them at any time. A pipeline that
must run unattended should be ready to handle `BulkDownloadChallengeError`.

When ODA Reader meets a challenge it raises `BulkDownloadChallengeError`. Its message names
the file and prints the call that registers a copy downloaded in a browser:

```text
Cloudflare presented an interactive challenge for https://webfs-dcd.oecd.org/.../CRS.parquet.
Open that URL in a browser to download CRS.parquet, then register the file so
oda-reader reads it from the cache:

    import oda_reader
    oda_reader.register_bulk_file('~/Downloads/CRS.parquet', dataset='crs', url='https://webfs-dcd.oecd.org/.../CRS.parquet', version='"5f1c..."')
```

The error also carries these values as attributes: `url`, `filename`,
`dataset` (set when a named reader such as `bulk_download_crs()` made the
request), `version` (the host's token, when it could be read) and `cf_ray`.

#### Register a file downloaded in a browser

Open the URL in a browser, let it save the file, then register it:

```python
import oda_reader

oda_reader.register_bulk_file(
    "~/Downloads/CRS.parquet", dataset="crs", url="<url from the error>"
)
crs = oda_reader.bulk_download_crs()  # reads the registered copy
```

`register_bulk_file` checks that the file is a valid zip or parquet payload,
copies it into the bulk file cache and returns the cached path. You can
delete the download afterwards. The registered copy does not expire. Online
reads keep serving it until OECD publishes a new version of the file.
`bulk_download_crs(offline=True)` reads it with no network request. A CRS year file registers with `url=` and reads
offline through `bulk_download_parquet(url=..., offline=True)`. See
[Registered files](caching.md#registered-files) and
[Reading offline](caching.md#reading-offline).

`dataset` takes one of these names:

| `dataset`       | Reader                                    |
| --------------- | ----------------------------------------- |
| `"crs"`         | `bulk_download_crs()`                     |
| `"crs-reduced"` | `bulk_download_crs(reduced_version=True)` |
| `"dac1"`        | `bulk_download_dac1()`                    |
| `"dac2a"`       | `bulk_download_dac2a()`                   |
| `"dac2b"`       | `bulk_download_dac2b()`                   |
| `"multisystem"` | `bulk_download_multisystem()`             |

Without `url`, ODA Reader looks the URL up in OECD's SDMX metadata, which
needs network access to `sdmx.oecd.org`. `version` is the file host's
version token. The call the error prints includes it when the reader could
read it.

If the browser download is also blocked, report the `cf_ray` value to OECD
and use a narrowly filtered API request instead.

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

**File not found / 404 errors**: Bulk-download URLs are stable, so a routine OECD republish doesn't cause this (see [Caching & Performance](caching.md#cache-invalidation-on-republish) for how a republish is detected instead). A 404 here usually means the specific dataflow version was retired. ODA Reader re-resolves the URL and retries once automatically when that happens. If it still fails, older CRS year-specific files use grouped years (e.g., "1995-99"), so check which grouping includes your target year.

**`BulkPayloadCorruptError`**: The OECD's bulk endpoint occasionally serves a
truncated or malformed file (a bad zip, or a bare parquet file that fails to
parse). The corrupt download is deleted before the exception is raised and
never replaces the cached copy, so the next call cleanly re-downloads. Retry the call, or pass
`use_raw_cache=False` to skip the cache for that invocation.

## Next steps

- **[Caching & Performance](caching.md)** - Understand how bulk downloads are cached
- **[Schema Translation](schema-translation.md)** - Learn about .Stat vs. API schema differences
- **[Filtering Data](filtering.md)** - Apply filters to bulk downloaded data using pandas
