# Filtering Data

All ODA Reader download functions accept a `filters` parameter that lets you query specific subsets of data. This page explains how filtering works across datasets.

## Basic Filtering Pattern

Filters use a dictionary where keys are dimension names and values are dimension codes:

```python
from oda_reader import download_dac1

# Filter for a single donor
data = download_dac1(
    start_year=2022,
    end_year=2022,
    filters={"donor": "USA"}
)
```

This pattern works across all datasets: DAC1, DAC2a, CRS, and Multisystem.

## Filtering with Multiple Values

Pass a list to filter for multiple values of the same dimension:

```python
# Filter for multiple donors
data = download_dac1(
    start_year=2022,
    end_year=2022,
    filters={"donor": ["USA", "GBR", "FRA"]}
)
```

The result includes rows matching any of the values (logical OR).

## Combining Multiple Dimensions

Specify multiple dimensions to narrow your query further:

```python
# Multiple dimensions: donor AND measure type AND price base
data = download_dac1(
    start_year=2022,
    end_year=2022,
    filters={
        "donor": "FRA",
        "measure": "11017",  # Grant equivalent of loans
        "flow_type": "1160",  # Net flows
        "price_base": "Q"  # Constant prices
    }
)
```

Multiple dimensions work as logical AND - results must match all specified filters.

## Discovering Available Filters

Each dataset has different dimensions. Use `get_available_filters()` to see what's available:

```python
from oda_reader import get_available_filters

# Get available filters for DAC1
dac1_filters = get_available_filters("dac1")
```

**Output:**
```
OrderedDict([('donor', typing.Union[str, list, NoneType]),
             ('recipient', typing.Union[str, list, NoneType]),
             ('flow_type', typing.Union[str, list, NoneType]),
             ('measure', typing.Union[str, list, NoneType]),
             ('unit_measure', typing.Union[str, list, NoneType]),
             ('price_base', typing.Union[str, list, NoneType])])
```

Each key is a dimension name you can use in `filters`. The type hints show you can pass a string, list, or None.

By default, `get_available_filters()` prints the result. To suppress printing:

```python
filters = get_available_filters("dac1", quiet=True)
```

**For other datasets:**

```python
# DAC2a filters
dac2a_filters = get_available_filters("dac2a")

# CRS filters
crs_filters = get_available_filters("crs")

# Multisystem filters
multisystem_filters = get_available_filters("multisystem")
```

## Dataset-Specific Filters

### DAC1 and DAC2a

Common dimensions:

- `donor` - Donor country (ISO3 codes like "USA", "GBR", "FRA")
- `recipient` - Recipient country or region (DAC2a only)
- `measure` - Type of flow (ODA, OOF, grants, loans, etc.)
- `flow_type` - Commitments, disbursements, net flows, etc.
- `price_base` - "V" for current prices, "Q" for constant prices
- `unit_measure` - "USD" for US dollars

**Example**: Get net ODA disbursements in constant prices:

```python
from oda_reader import download_dac1

data = download_dac1(
    start_year=2020,
    end_year=2022,
    filters={
        "measure": "1010",  # Net ODA
        "flow_type": "1140",  # Disbursements
        "price_base": "Q"  # Constant prices
    }
)
```

### CRS (Creditor Reporting System)

CRS has additional dimensions:

- `sector` - Purpose codes (5-digit codes like "12220" for basic health)
- `channel` - Implementing organization (government, NGO, multilateral, etc.)
- `modality` - Grant, loan, equity, etc.
- `microdata` - **Important**: `True` (default) for project-level, `False` for semi-aggregates

**Example**: Get microdata for education sector from Germany:

```python
from oda_reader import download_crs

education_projects = download_crs(
    start_year=2022,
    end_year=2022,
    filters={
        "donor": "DEU",
        "sector": "110"  # Education (3-digit aggregate)
    }
)
# Returns individual projects (microdata=True by default)
```

### CRS Semi-Aggregates

The online OECD Data Explorer shows semi-aggregated CRS data, not microdata. To match that view:

1. Set `microdata: False`
2. Specify `channel: "_T"` (total across channels)
3. Specify `modality: "_T"` (total across modalities)

**Example**: Get semi-aggregated data matching Data Explorer:

```python
# Semi-aggregated totals (matches online Data Explorer)
semi_agg = download_crs(
    start_year=2022,
    end_year=2022,
    filters={
        "donor": "USA",
        "recipient": "NGA",
        "microdata": False,
        "channel": "_T",  # Total - required for semi-aggregates
        "modality": "_T"  # Total - required for semi-aggregates
    }
)
```

The `_T` suffix means "total" - it aggregates across that dimension to avoid double-counting.

### Multisystem

Multisystem tracks multilateral contributions:

- `donor` - Contributing country
- `channel` - Specific multilateral organization (e.g., "44002" for World Bank IDA)
- `flow_type` - Commitments, disbursements
- `measure` - Core contributions, earmarked funds, etc.

**Example**: Get contributions to UN agencies from Canada:

```python
from oda_reader import download_multisystem

un_contributions = download_multisystem(
    start_year=2020,
    end_year=2022,
    filters={
        "donor": "CAN",
        "channel": "41000"  # UN agencies (aggregate code)
    }
)
```

## Filter Code Lookup

Filter values use codes, not human-readable names. To find the right codes:

1. **Download without filters** and inspect unique values:

```python
data = download_dac1(start_year=2022, end_year=2022)
print(data['donor'].unique())  # See all donor codes
print(data['measure'].unique())  # See all measure codes
```

2. **Check OECD documentation**: Code lists are in the [OECD DAC Glossary](https://www.oecd.org/dac/financing-sustainable-development/development-finance-standards/)

3. **Use trial and error**: Download a small query and examine column values

**Note**: Codes differ between API schema and .Stat schema. When making API calls, you must use the
API schema. However by default, ODA Reader returns .Stat codes. See [Schema Translation](schema-translation.md) for details.

## Empty Filters

Pass an empty dictionary or omit `filters` to get all data (subject to year range):

```python
# These are equivalent - both return all data
data1 = download_dac1(start_year=2022, end_year=2022)
data2 = download_dac1(start_year=2022, end_year=2022, filters={})
```

Be careful with unfiltered CRS queries - they can be very large and slow.

## Common Filtering Patterns

### By donor and recipient

```python
# Bilateral flows from US to Kenya
us_to_kenya = download_dac2a(
    start_year=2020,
    end_year=2022,
    filters={
        "donor": "USA",
        "recipient": "KEN"
    }
)
```

### By sector

```python
# Health sector activities from all donors
health = download_crs(
    start_year=2022,
    end_year=2022,
    filters={"sector": "120"}  # Health (3-digit)
)
```

### By multiple criteria

```python
# French education grants to West Africa in constant prices
french_education = download_crs(
    start_year=2020,
    end_year=2022,
    filters={
        "donor": "FRA",
        "recipient": "298",  # West Africa (regional code)
        "sector": "110",  # Education
        "modality": "100",  # Grants
        "price_base": "Q"  # Constant prices
    }
)
```

## Troubleshooting

**Query returns no data**: Check your filter codes are valid. Try removing filters one by one to identify the issue.

**Query is slow**: The CRS API is inherently slow for large queries. Consider using [bulk downloads](bulk-downloads.md) instead.

**Filter not working**: Make sure the dimension name matches exactly (case-sensitive). Use `get_available_filters()` to verify spelling.

**Unexpected results**: Remember that multiple values in one filter are OR, but multiple filters are AND. Also check if you need semi-aggregates (CRS `microdata: False`).

## Next Steps

- **[Bulk Downloads](bulk-downloads.md)** - Download large datasets efficiently
- **[Schema Translation](schema-translation.md)** - Understand filter code differences between schemas
- **[Datasets Overview](datasets.md)** - Learn which dimensions each dataset offers
