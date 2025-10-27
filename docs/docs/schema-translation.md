# Schema Translation

OECD DAC data exists in two schema formats: the modern Data Explorer API schema and the legacy OECD.Stat schema. ODA Reader can translate between them.

## The Two Schemas

### Data Explorer API Schema (New)

The current OECD Data Explorer uses a new schema:

- Column names: `DONOR`, `RECIPIENT`, `MEASURE`, etc. (all caps)
- Dimension codes: Modern conventions (e.g., donor codes)
- Used by: API downloads (`download_dac1()`, `download_crs()`, etc.)

### OECD.Stat Schema (Legacy)

The older OECD.Stat system uses a different schema:

- Column names: `DonorCode`, `RecipientCode`, `Measure`, etc. (mixed case)
- Dimension codes: Legacy conventions, sometimes different from API codes
- Used by: Bulk download files, historical .Stat exports

**Why two schemas?** OECD transitioned from .Stat to Data Explorer but kept bulk files in the old format. Both are still in use.

## Translation Parameters

ODA Reader provides two parameters to control schema handling:

### `pre_process` (default: `True`)

Performs basic cleaning:
- Renames columns to machine-readable names (e.g., `DONOR` → `donor_code`)
- Sets proper data types (`int`, `float`, `string`)
- Removes empty columns

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

print(data_raw.columns)
# Output: ['DONOR', 'RECIPIENT', 'MEASURE', 'AMOUNT_TYPE', 'FLOW_CODE', ...]
# (exact API column names, all caps)
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

print(data_clean.columns)
# Output: ['donor', 'recipient', 'measure', 'amount_type', 'flow_code', ...]
# (clean, consistent naming)
```

### `dotstat_codes` (default: `True`)

Translates dimension **codes** from API format to .Stat format:
- Requires `pre_process=True` to work
- Converts codes like donor IDs, measure types, flow codes
- Makes data compatible with .Stat bulk downloads and historical data

**Example without code translation**:

```python
# API codes only
data_api = download_dac1(
    start_year=2022,
    end_year=2022,
    pre_process=True,
    dotstat_codes=False
)

print(data_api['donor'].unique()[:5])
# Output: ['1', '2', '3', '4', '5']
# (numeric API codes)
```

**Example with code translation**:

```python
# Translated to .Stat codes (default)
data_stat = download_dac1(
    start_year=2022,
    end_year=2022,
    pre_process=True,
    dotstat_codes=True
)

print(data_stat['donor'].unique()[:5])
# Output: ['AUS', 'AUT', 'BEL', 'CAN', 'CHE']
# (ISO3 donor codes, matches .Stat format)
```

## Three Modes Explained

### Mode 1: Default (Recommended)

```python
data = download_dac1(start_year=2022, end_year=2022)
# Equivalent to:
# pre_process=True, dotstat_codes=True
```

**Result**:
- Clean column names: `donor`, `recipient`, `measure`, etc.
- .Stat codes: `'USA'`, `'GBR'` for donors
- Proper data types set
- **Use when**: General analysis, compatibility with historical .Stat data

**Pros**:
- Works with existing .Stat-based workflows
- Compatible with bulk download files

### Mode 2: Raw API Response

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
- Raw API codes: numeric or internal codes
- No type conversion
- **Use when**: Debugging API issues, understanding API structure

**Pros**:
- See exactly what OECD API returns
- Useful for troubleshooting

**Cons**:
- Harder to work with (inconsistent naming)

### Mode 3: Preprocessed with API Codes

```python
data = download_dac1(
    start_year=2022,
    end_year=2022,
    pre_process=True,
    dotstat_codes=False
)
```

**Result**:
- Clean column names: `donor`, `recipient`, etc.
- API codes: numeric or internal (not .Stat codes)
- Proper data types
- **Use when**: Working exclusively with new API data, don't need .Stat compatibility

**Pros**:
- Clean DataFrame structure
- Uses OECD's latest code conventions

**Cons**:
- Codes differ from .Stat bulk files
- May not match historical datasets

## Code Translation Examples

### Donor Codes

| .Stat Code | API code | Country |
|----------|----------|---------|
| `1` | `AUS`    | Australia |
| `2` | `AUT`    | Austria |
| `12` | `USA`    | United States |
| `301` | `GBR`    | United Kingdom |

### Measure Codes (DAC1)

| .Stat Code | API Code | Description |
|------------|------------|-------------|
| `100`      | `1010` | Net ODA |
| `106`      | `1011` | ODA Grants |
| `11017`    | `11017` | Grant equiv. of loans |

(Note: Some codes are the same across schemas)


Translation mappings are maintained in `src/oda_reader/schemas/mappings/` as JSON files.

## Bulk Download Schema

**Important**: Bulk downloads (CRS, Multisystem, AidData) **always use .Stat schema**.

```python
from oda_reader import bulk_download_crs

# This always returns .Stat schema
crs_bulk = bulk_download_crs()

print(crs_bulk.columns)
# Output: ['DonorCode', 'RecipientCode', 'SectorCode', ...]
# (mixed case, .Stat conventions)
```

There are no `pre_process` or `dotstat_codes` parameters for bulk downloads - the files are already in .Stat format.

**Combining API and bulk downloads**:

If you mix API (with `.Stat codes) and bulk downloads, they should be compatible. But column **names** may differ slightly:

```python
# API download with .Stat codes
api_data = download_crs(
    start_year=2023,
    end_year=2023,
    pre_process=True,
    dotstat_codes=True
)

# Bulk download (always .Stat)
bulk_data = bulk_download_crs()

# Column names differ slightly:
print(api_data.columns[:5])  # ['donor', 'recipient', 'year', 'sector', ...]
print(bulk_data.columns[:5])  # ['DonorCode', 'RecipientCode', 'Year', 'SectorCode', ...]

# But codes are the same (both use .Stat codes):
print(api_data['donor'].unique()[:3])  # ['USA', 'GBR', 'DEU']
print(bulk_data['DonorCode'].unique()[:3])  # ['USA', 'GBR', 'DEU']
```

You can harmonize column names with pandas:

```python
# Rename bulk columns to match API preprocessing
bulk_data = bulk_data.rename(columns={
    'DonorCode': 'donor',
    'RecipientCode': 'recipient',
    # ... etc
})
```

## When to Use Which Mode

**Use default mode (pre_process=True, dotstat_codes=True)**:

-  General analysis and research
-  Combining API downloads with bulk files
-  Working with historical .Stat exports
-  Human-readable codes (ISO3 country codes)

**Use raw mode (pre_process=False, dotstat_codes=False)**:
-  Debugging API issues
- Understanding API response structure

**Use API codes mode (pre_process=True, dotstat_codes=False)**:
-  Working exclusively with new Data Explorer API
-  When you prefer OECD's latest code conventions
-  Avoid if combining with bulk downloads or .Stat files

## Finding Code Mappings

Code translation mappings are defined in:
```
src/oda_reader/schemas/mappings/
├── dac1_mapping.json
├── dac2_mapping.json
├── crs_mapping.json
└── multisystem_mapping.json
```

Each file maps API codes to .Stat codes for that dataset's dimensions.

**Example from `dac1_mapping.json`**:

```json
{
  "DONOR": {
    "keep": true,
    "name": "donor",
    "type": "string[pyarrow]",
    "mapping": {
      "1": "AUS",
      "2": "AUT",
      "12": "USA"
    }
  }
}
```

You can inspect these files to understand how specific codes translate.

## Troubleshooting

**Codes don't match between downloads**:
- Check if one is API download and other is bulk download
- Verify `dotstat_codes=True` for API downloads when combining with bulk
- Column names differ even with same codes - rename if needed

**"Translation failed" errors**:
- Ensure `pre_process=True` when using `dotstat_codes=True`
- Some newer API codes may not have .Stat mappings yet
- File an issue if you encounter unmapped codes

**Unexpected column names**:
- Check `pre_process` setting - raw API uses all-caps names
- Bulk downloads have their own naming (can't be changed)

## Next Steps

- **[Bulk Downloads](bulk-downloads.md)** - Understand bulk download schema
- **[Filtering Data](filtering.md)** - Use the right codes for filtering
- **[Advanced Topics](advanced.md)** - Custom schema handling
