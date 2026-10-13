---
description: How the pre_process and dotstat_codes options convert Data Explorer API columns and codes to the OECD.Stat schema, and which mode to pick.
---

# Schema Translation

OECD DAC data exists in two schema formats: the modern Data Explorer API schema and the legacy OECD.Stat schema. ODA Reader can translate between them.

## The two schemas

### Data Explorer API schema (new)

The current OECD Data Explorer uses a new schema:

- Column names: `DONOR`, `RECIPIENT`, `MEASURE`, etc. (all caps)
- Dimension codes: the API returns ISO3 donor codes such as `AUT`
- Used by: API downloads (`download_dac1()`, `download_crs()`, etc.)

### OECD.Stat schema (legacy)

The older OECD.Stat system uses a different schema:

- Column names: `DonorCode`, `RecipientCode`, `Measure`, etc. (mixed case), except the full CRS
  parquet files (`bulk_download_crs()`), which use snake_case. See
  [Bulk download schema](#bulk-download-schema) below for the split.
- Dimension codes: Legacy conventions, sometimes different from API codes
- Used by: Bulk download files, historical .Stat exports

**Why two schemas?** OECD transitioned from .Stat to Data Explorer but kept bulk files in the old format. Both are still in use.

## Translation parameters

ODA Reader provides two parameters to control schema handling:

### `pre_process` (default: `True`)

Performs basic cleaning:

- Renames columns to machine-readable names (e.g., `DONOR` → `donor_code`)
- Sets proper data types (`int`, `float`, `string`)
- Drops columns the schema marks as not kept

**Example without preprocessing**:

```python
from oda_reader import download_dac1

# Raw API response
data_raw = download_dac1(
    start_year=2022,
    end_year=2022,
    pre_process=False,
    dotstat_codes=False
)

print([c for c in ["DONOR", "Donor", "MEASURE", "Measure"] if c in data_raw.columns])
# Output: ['DONOR', 'Donor', 'MEASURE', 'Measure']
# (exact API column names: codes in capitals, labels in title case)
```

**Example with preprocessing**:

```python
# Preprocessed (default)
data_clean = download_dac1(
    start_year=2022,
    end_year=2022,
    pre_process=True,
    dotstat_codes=False
)

print(list(data_clean.columns))
# Output: ['donor_code', 'donor_name', 'aidtype_code', 'aid_type', 'flows_code', ...]
# (clean, consistent naming)
```

### `dotstat_codes` (default: `True`)

Translates dimension **codes** from API format to .Stat format:

- Requires `pre_process=True` to work
- Converts donor and recipient area codes and the price-base code, plus DAC2b measure codes
- Makes data compatible with .Stat bulk downloads and historical data

**Example without code translation**:

```python
# API codes only
data_api = download_dac1(
    start_year=2022,
    end_year=2022,
    filters={"donor": "USA"},
    pre_process=True,
    dotstat_codes=False
)

print(list(data_api['donor_code'].unique()))
# Output: ['USA']
# (ISO3 API code)
```

**Example with code translation**:

```python
# Translated to .Stat codes (default)
data_stat = download_dac1(
    start_year=2022,
    end_year=2022,
    filters={"donor": "USA"},
    pre_process=True,
    dotstat_codes=True
)

print(list(data_stat['donor_code'].unique()))
# Output: [302]
# (numeric .Stat donor code)
```

## Three modes explained

### Mode 1: Default (Recommended)

```python
data = download_dac1(start_year=2022, end_year=2022)
# Equivalent to:
# pre_process=True, dotstat_codes=True
```

**Result**:

- Clean column names: `donor_code`, `donor_name`, `aidtype_code`, etc.
- .Stat codes: numeric donor codes such as `302` for the United States
- Proper data types set
- **Use when**: General analysis, compatibility with historical .Stat data

**Pros**:

- Works with existing .Stat-based workflows
- Compatible with bulk download files

### Mode 2: Raw API response

```python
data = download_dac1(
    start_year=2022,
    end_year=2022,
    pre_process=False,
    dotstat_codes=False
)
```

**Result**:

- Raw API column names: `DONOR`, `MEASURE` (all caps)
- Raw API codes: ISO3 donor codes such as `AUT`
- No type conversion
- **Use when**: Debugging API issues, understanding API structure

**Pros**:

- See exactly what OECD API returns
- Useful for troubleshooting

**Cons**:

- Harder to work with (inconsistent naming)

### Mode 3: Preprocessed with API codes

```python
data = download_dac1(
    start_year=2022,
    end_year=2022,
    pre_process=True,
    dotstat_codes=False
)
```

**Result**:

- Clean column names: `donor_code`, `donor_name`, etc.
- API codes: ISO3 donor codes such as `AUT`
- Proper data types
- **Use when**: Working exclusively with new API data, don't need .Stat compatibility

**Pros**:

- Clean DataFrame structure
- Uses OECD's latest code conventions

**Cons**:

- Codes differ from .Stat bulk files
- May not match historical datasets

## Code translation examples

### Donor codes

| API code | .Stat code | Country        |
| -------- | ---------- | -------------- |
| `AUT`    | `1`        | Austria        |
| `BEL`    | `2`        | Belgium        |
| `GBR`    | `12`       | United Kingdom |
| `CAN`    | `301`      | Canada         |
| `USA`    | `302`      | United States  |
| `AUS`    | `801`      | Australia      |

DAC1 measure codes, such as `1010` for net ODA, pass through unchanged.

Translation mappings are maintained in `src/oda_reader/schemas/mappings/` as JSON files.

## Bulk download schema

**Important**: Bulk downloads (CRS, Multisystem, AidData) **always use .Stat codes**. Column
*naming* is not as uniform: most bulk files use PascalCase column names, but the full CRS
parquet files (`bulk_download_crs()`) use snake_case instead. See
[Bulk Downloads: column casing](bulk-downloads.md#bulk_download_crs-uses-different-column-casing-than-everything-else)
for why.

```python
from oda_reader import bulk_download_crs, download_crs_file

# bulk_download_crs() downloads a bare parquet file: snake_case columns
crs_bulk = bulk_download_crs()
print(list(crs_bulk.columns)[:5])
# ['year', 'donor_code', 'de_donorcode', 'donor_name', 'agency_code']

# download_crs_file() downloads a zip: PascalCase columns, same .Stat codes
crs_year = download_crs_file(year=2022)
print(list(crs_year.columns)[:5])
# ['Year', 'DonorCode', 'DEDonorcode', 'DonorName', 'AgencyCode']
```

There are no `pre_process` or `dotstat_codes` parameters for bulk downloads - the files are already in .Stat format.

**Combining API and bulk downloads**:

If you mix API (with .Stat codes) and bulk downloads, they should be compatible. But column
**names** differ, and differ by which bulk function you used:

```python
# API download with .Stat codes
api_data = download_crs(
    start_year=2023,
    end_year=2023,
    pre_process=True,
    dotstat_codes=True
)

# Bulk download (always .Stat codes, snake_case columns for the full CRS)
bulk_data = bulk_download_crs()

# Column names differ:
print(list(api_data.columns[:5]))   # ['donor_code', 'donor_name', 'recipient_code', 'recipient_name', 'sector_code']
print(list(bulk_data.columns[:5]))  # ['year', 'donor_code', 'de_donorcode', 'donor_name', 'agency_code']

# But codes are the same (both use .Stat codes):
print(list(api_data['donor_code'].unique()[:3]))   # .Stat numeric codes, such as 302 for the United States
print(list(bulk_data['donor_code'].unique()[:3]))  # the same .Stat codes
```

You can harmonise the names that differ with pandas:

```python
# Rename bulk columns to match API preprocessing
bulk_data = bulk_data.rename(columns={
    'bulk_column_name': 'api_column_name',
    # ... etc
})
```

## When to use which mode

**Use default mode (pre_process=True, dotstat_codes=True)**:

- General analysis and research
- Combining API downloads with bulk files
- Working with historical .Stat exports
- Numeric .Stat donor codes, as in the bulk files

**Use raw mode (pre_process=False, dotstat_codes=False)**:

- Debugging API issues
- Understanding API response structure

**Use API codes mode (pre_process=True, dotstat_codes=False)**:

- Working exclusively with new Data Explorer API
- When you prefer OECD's latest code conventions
- Avoid if combining with bulk downloads or .Stat files

## Finding code mappings

Code translation mappings are defined in:

```
src/oda_reader/schemas/mappings/
├── dac1_dotstat.json
├── dac2_dotstat.json
├── crs_dotstat.json
├── multisystem_dotstat.json
├── dac1_codes_area.json
└── dac2_codes_area.json
```

The four `<dataset>_dotstat.json` files above define each dataset's columns: the API name, the clean name, the type, and whether to keep the column. The other JSON files in the folder hold code maps, among them `dac1_codes_area.json`, `dac1_codes_prices.json`, and `dac2b_measure_dotstat.json`.

**Example from `dac1_dotstat.json`**:

```json
{
  "DONOR": {
    "name": "donor_code",
    "type": "string[pyarrow]",
    "keep": true
  }
}
```

**Example from `dac1_codes_area.json`**, which maps .Stat codes to API codes:

```json
{
  "1": "AUT",
  "2": "BEL",
  "12": "GBR"
}
```

You can inspect these files to understand how specific codes translate.

## Troubleshooting

**Codes don't match between downloads**:

- Check if one is API download and other is bulk download
- Verify `dotstat_codes=True` for API downloads when combining with bulk
- Column names differ even with same codes - rename if needed

**"Cannot convert to dotstat codes without preprocessing" errors**:

- Set `pre_process=True` when using `dotstat_codes=True`

**"Unmapped DAC2B measure code(s)" errors**:

- A new API measure code has no .Stat mapping yet. File an issue

**Unexpected column names**:

- Check `pre_process` setting - raw API uses all-caps names
- Bulk downloads have their own naming (can't be changed)

## Next steps

- **[Bulk Downloads](bulk-downloads.md)** - Understand bulk download schema
- **[Filtering Data](filtering.md)** - Use the right codes for filtering
- **[Advanced Topics](advanced.md)** - Custom schema handling
