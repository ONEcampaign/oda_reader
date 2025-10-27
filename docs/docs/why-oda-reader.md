# Why ODA Reader?

This page explains why ODA Reader exists, how it compares to alternatives, and when you might want to use it (or not).

## The Problem with OECD DAC Data Access

The OECD Development Assistance Committee publishes comprehensive data on official development assistance, but accessing it programmatically is unnecessarily difficult:

**No official Python library**: The OECD doesn't provide any first-party Python tools for accessing DAC data. You're on your own to figure out the SDMX API, construct queries, and parse responses.

**Undocumented breaking changes**: The OECD regularly introduces schema changes without documentation or warning. A dataflow version that worked last month might return 404 errors today. Link URLs change, breaking saved bookmarks and automated downloads.

**Inconsistent formats**: Different datasets use different schemas. The new Data Explorer API uses one set of dimension codes, while legacy .Stat files and bulk downloads use another. Reconciling these takes significant effort.

**Complex API syntax**: SDMX queries require precise filter strings where dimension order matters and syntax varies between API versions. One wrong character breaks the entire query.

## Alternatives and Comparisons

### Direct SDMX API Usage

**Approach**: Construct HTTP requests to OECD's SDMX endpoints manually.

**Challenges**:
- No Python library means writing your own HTTP client code
- Complex URL construction: `https://sdmx.oecd.org/public/rest/v2/data/dataflow/OECD.DCD.FSD/DF_CRS/1.0/USA.NGA....11220?startPeriod=2020&endPeriod=2022`
- Manual rate limiting required or risk getting blocked
- Schema changes break queries without warning
- No automatic retries or fallback mechanisms

**What ODA Reader provides**: Automatic version fallback, rate limiting built-in, consistent function interface, handles schema changes gracefully.

### Manual Downloads from OECD.Stat

**Approach**: Download CSV or Excel files from OECD.Stat portal manually.

**Challenges**:
- No automation - manual clicking and downloading
- Portal URLs change, bookmarks break
- File format inconsistencies between download dates
- No programmatic filtering or querying
- Time-consuming for iterative analysis

**What ODA Reader provides**: Programmatic bulk downloads with consistent interfaces, automatic format handling, filterable queries, easy iteration.

### Using Generic SDMX Libraries

**Approach**: Use general-purpose SDMX Python libraries like `pandasdmx`.

**Challenges**:
- Generic libraries don't handle DAC-specific quirks
- No built-in knowledge of which datasets exist or their schemas
- Schema translation between API and .Stat formats still manual
- No automatic handling of OECD's breaking changes
- Steeper learning curve for SDMX concepts

**What ODA Reader provides**: DAC-specific functions (`download_dac1`, `download_crs`), automatic schema translation, dataset-specific documentation, simpler API for common tasks.

## Design Decisions

### Why Both API and Bulk Downloads?

**API downloads** are ideal for:
- Filtered queries (specific donors, recipients, years)
- Exploratory analysis
- Smaller datasets (DAC1, DAC2a)

**Bulk downloads** are ideal for:
- Full CRS dataset (millions of rows)
- Avoiding slow API calls and rate limits
- Reproducible research requiring exact dataset versions

ODA Reader provides both because different analysis workflows need different approaches.

### Why Automatic Caching?

API calls to OECD are slow (often 10-30 seconds per query) and subject to rate limiting. Caching means:
- Repeated queries are instant
- Less dependency on OECD's server reliability
- Iterative analysis doesn't hit rate limits
- Reproducibility - cached data doesn't change

You can disable or clear caching when you need fresh data.

### How Version Fallback Works

When OECD changes a dataflow schema version, ODA Reader:
1. Tries the configured version (e.g., `1.0`)
2. If 404 error, automatically retries with `0.9`
3. Continues decrementing (0.8, 0.7, 0.6) up to 5 attempts
4. Returns data from first successful version

This means your code keeps working even when OECD makes breaking changes.

## Limitations and When Not to Use ODA Reader

**Be honest about limitations:**

❌ **Not for real-time data**: Caching introduces delays. If you need the absolute latest data published in the last hour, you'll need to clear cache or use the OECD portal directly.

❌ **Requires Python knowledge**: This is a Python package. If you're not comfortable with Python and pandas, the OECD.Stat portal's Excel downloads might be easier.

❌ **Only covers DAC data**: ODA Reader focuses exclusively on Development Assistance Committee datasets. For other OECD data (economic indicators, education statistics, etc.), you'll need different tools.

❌ **Bulk downloads limited**: Only CRS, Multisystem, and AidData have bulk download options. For other datasets, you must use the API.

❌ **Dependent on OECD availability**: While caching helps, initial downloads still depend on OECD's servers being available and responsive.

## When to Use ODA Reader

✅ You're doing research or analysis that requires ODA/OOF data

✅ You need programmatic, reproducible access to multiple datasets

✅ You're building data pipelines that need to be robust to OECD's changes

✅ You want to avoid manually managing API rate limits and caching

✅ You need to work with both API and bulk download formats

✅ You're comfortable with Python and pandas

## Next Steps

Ready to try it? Head to [Getting Started](getting-started.md) to install ODA Reader and run your first queries.
