# Getting Started with ODA Reader

ODA Reader provides simple Python functions to download OECD DAC data. This page walks you through installation and your first queries.

## Installation

Install ODA Reader from PyPI using pip:

```bash
pip install oda-reader
```

Or using uv (recommended for faster installs):

```bash
uv add oda-reader
```

That's it! ODA Reader and its dependencies (pandas, requests, pyarrow, etc.) are now installed.

## Your First Query: Download DAC1 Data

Let's download aggregate ODA data from DAC1 for a single year:

```python
from oda_reader import download_dac1

# Download all available DAC1 data for 2022
data = download_dac1(start_year=2022, end_year=2022)

print(f"Downloaded {len(data)} rows")
print(data.head())
```

**Output:**
```
Downloaded 40119 rows
   donor_code   donor_name  aidtype_code                               aid_type  flows_code          fund_flows amounttype_code     amount_type sector_code     sector_name  year      value  base_period  unit_multiplier
0       20000  DAC Members             5             Official and private flows        1140  Disbursements, net               A  Current prices        <NA>  Not applicable  2022  510041.33         <NA>                6
1       20000  DAC Members          1010  Official Development Assistance (ODA)        1140  Disbursements, net               A  Current prices        <NA>  Not applicable  2022  240675.09         <NA>                6
2       20000  DAC Members          1015                          Bilateral ODA        1140  Disbursements, net               A  Current prices        <NA>  Not applicable  2022   190247.3         <NA>                6
```

The function returns a pandas DataFrame with columns for donor, measure type, flow type, amount, and more. By default, ODA Reader:

- Preprocesses column names to be machine-readable
- Converts to OECD.Stat schema codes for compatibility
- Caches results for fast repeated queries

## Filtering Your Query

You can filter downloads to specific dimensions. Let's get data for just the USA and UK:

```python
from oda_reader import download_dac1

# Filter for specific donors
data = download_dac1(
    start_year=2022,
    end_year=2022,
    filters={"donor": ["USA", "GBR"]}
)

print(f"Downloaded {len(data)} rows for USA and GBR")
print(data['donor_name'].unique())
```

**Output:**
```
Downloaded 1851 rows for USA and GBR
['United States', 'United Kingdom']
```

Filters use a dictionary where keys are dimension names and values are codes (single value or list). This pattern works across all datasets.

## Download a Different Dataset: DAC2a

DAC2a contains bilateral flows by recipient. Let's download data for specific recipients:

```python
from oda_reader import download_dac2a

# Download DAC2a data for Nigeria and Kenya
data = download_dac2a(
    start_year=2022,
    end_year=2022,
    filters={"recipient": ["NGA", "KEN"]}
)

print(f"Downloaded {len(data)} rows")
print(f"Recipients: {sorted(data['recipient_name'].unique())}")
```

**Output:**
```
Downloaded 3724 rows
Recipients: ['Kenya', 'Nigeria']
```

DAC2a includes recipient countries as a dimension, making it ideal for analyzing who receives aid from whom.

## What Just Happened?

When you ran these examples:

1. **ODA Reader constructed SDMX API queries** - You didn't need to know the complex API syntax
2. **Results were cached** - Run the same query again and it's instant
3. **Rate limiting was applied** - Automatic pauses prevent you from hitting API limits
4. **Schema translation happened** - Codes were converted to .Stat format for compatibility

## Next Steps

Now that you've downloaded your first datasets, explore:

- **[Datasets Overview](datasets.md)** - Learn about all 5 available datasets and when to use each
- **[Filtering Data](filtering.md)** - Discover available filters and build complex queries
- **[Bulk Downloads](bulk-downloads.md)** - Download full datasets efficiently for large-scale analysis
- **[Caching & Performance](caching.md)** - Manage cache and configure rate limiting

## Troubleshooting

**Query is slow**: First-time queries can take 10-30 seconds as ODA Reader fetches from OECD's API. Subsequent identical queries are instant due to caching.

**Rate limit errors**: By default, ODA Reader limits to 20 requests per hour. This should prevent rate limit errors. If you see them, your cache might have been cleared. Wait and retry.
