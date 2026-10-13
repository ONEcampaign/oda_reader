---
description: Understand the access problems ODA Reader addresses, compare it with direct downloads and SDMX libraries, and decide when another tool fits better.
---

# Why ODA Reader?

## The problem with OECD DAC data access

The OECD Development Assistance Committee publishes comprehensive data on official development assistance, but accessing it programmatically is unnecessarily difficult:

**No official Python library**: The OECD doesn't provide any first-party Python tools for accessing DAC data. You're on your own to figure out the SDMX API, construct queries, and parse responses.

**Undocumented breaking changes**: The OECD regularly introduces schema changes without documentation or warning. A dataflow version that worked last month might return 404 errors today, or, worse, return outdated data. Link URLs change, breaking saved bookmarks and automated downloads.

**Inconsistent formats**: Different datasets use different schemas. The new Data Explorer API uses one set of dimension codes, while legacy .Stat files and bulk downloads use another. Reconciling these takes significant effort.

**Complex API syntax**: SDMX queries require precise filter strings where dimension order matters and syntax varies between API versions. One wrong character breaks the entire query.

## Alternatives and comparisons

### Direct SDMX API usage

**Approach**: Construct HTTP requests to OECD's SDMX endpoints manually.

**Challenges**:

- No Python library means writing your own HTTP client code
- Complex URL construction. ODA Reader builds this one for US flows to Nigeria under purpose code 11220: `https://sdmx.oecd.org/dcd-public/rest/data/OECD.DCD.FSD,DSD_CRS@DF_CRS,1.6/USA.NGA.11220......DD..?format=csvfilewithlabels&startPeriod=2020&endPeriod=2022`
- Manual rate limiting required or risk getting blocked
- Schema changes break queries without warning
- No automatic retries or fallback mechanisms

**What ODA Reader provides**: Automatic version discovery, rate limiting built-in, consistent function interface, handles schema changes gracefully.

### Manual downloads from OECD.Stat

**Approach**: Download Parquet, CSV or Excel files from the data-explorer manually.

**Challenges**:

- No automation: manual clicking and downloading
- Portal URLs change, bookmarks break
- File format inconsistencies between download dates
- No programmatic filtering or querying
- Time-consuming for iterative analysis

**What ODA Reader provides**: Programmatic bulk downloads with consistent interfaces, automatic format handling, filterable queries, easy iteration.

### Using generic SDMX libraries

**Approach**: Use general-purpose SDMX Python libraries like `pandasdmx`.

**Challenges**:

- Generic libraries don't handle DAC-specific quirks
- No built-in knowledge of which datasets exist or their schemas
- Schema translation between API and .Stat formats still manual
- No automatic handling of OECD's breaking changes
- Steeper learning curve for SDMX concepts

**What ODA Reader provides**: DAC-specific functions (`download_dac1`, `download_crs`), automatic schema translation, dataset-specific documentation, simpler API for common tasks.

## Design decisions

### Why both API and bulk downloads?

**API downloads** are ideal for:

- Filtered queries (specific donors, recipients, years)
- Exploratory analysis
- Smaller datasets (DAC1, DAC2a, DAC2b)

**Bulk downloads** are ideal for:

- Full CRS dataset (millions of rows)
- Avoiding slow API calls and rate limits
- Reproducible research requiring exact dataset versions

ODA Reader provides both because different analysis workflows need different approaches.

### Why automatic caching?

API calls to OECD are slow (often 10-30 seconds per query) and subject to rate limiting. Caching means:

- Repeated queries are instant
- Less dependency on OECD's server reliability
- Iterative analysis doesn't hit rate limits
- Reproducibility: a cached API query returns the same DataFrame until you clear the cache or upgrade `oda_reader`

You can disable or clear caching when you need fresh data.

## Limitations and when not to use ODA Reader

**Requires Python knowledge**: This is a Python package. If you're not comfortable with Python and pandas, the OECD.Stat portal's Excel downloads might be easier.

**Mostly focused on DAC data**: ODA Reader focuses on Development Assistance Committee datasets. However, ODA Reader also covers AidData data.

**Dependent on OECD availability**: While caching helps, initial downloads still depend on OECD's servers being available and responsive.

## When to use ODA Reader

- You're doing research or analysis that requires ODA/OOF data
- You need programmatic, reproducible access to multiple datasets
- You're building data pipelines that need to be robust to OECD's changes
- You want to avoid manually managing API rate limits and caching
- You need to work with both API and bulk download formats
- You're comfortable with Python and pandas

## Next steps

Ready to try it? Head to [Getting Started](getting-started.md) to install ODA Reader and run your first queries.
