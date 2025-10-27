# ODA Reader

**Programmatic access to OECD DAC data**

Working with OECD Development Assistance Committee (DAC) data can be frustrating. You need to navigate multiple datasets (DAC1, DAC2a, CRS,...), understand complex SDMX API syntax, manage toy rate limits, and reconcile different schema versions. The OECD doesn't provide any first-party Python library to help.

Unfortunately, the OECD has a habit of introducing undocumented schema changes, breaking link URLs, and making format changes without notice. What works today might break tomorrow, making it very difficult to build robust data pipelines for research and analysis.

ODA Reader eliminates these headaches. It provides a unified Python interface that handles complexity for you: automatic search of the latest schema, consistent APIs across datasets, smart caching to reduce dependency on flaky endpoints, and schema translation between the data-explorer API and OECD.Stat formats.

**Key features**:

- **Access 5+ datasets** through simple functions: DAC1, DAC2a, CRS, Multisystem, AidData
- **Apply filters easily**: `filters={"donor": "USA", "recipient": "NGA"}` works across datasets
- **Bulk download large files** with memory-efficient streaming for the full CRS (1GB+)
- **Automatic rate limiting** and caching to work within API constraints
- **Schema translation** between Data Explorer API and OECD.Stat formats
- **Version fallback** automatically searches for the most recent schema version since they
can unexpectedly change with new data releases.

**Built for researchers, analysts, and developers** who need reliable, programmatic access to ODA data without fighting infrastructure.

## Quick Example

```python
from oda_reader import download_dac1

# Download aggregate ODA flows from 2020-2022
data = download_dac1(start_year=2020, end_year=2022)

# Filter for specific donors
us_uk_data = download_dac1(
    start_year=2020,
    end_year=2022,
    filters={"donor": ["USA", "GBR"]}
)
```

## Next Steps

- [Why ODA Reader](why-oda-reader.md) - Understand the rationale and compare to alternatives
- [Getting Started](getting-started.md) - Install and run your first queries in 5 minutes
- [Datasets Overview](datasets.md) - Learn about the 5 available datasets
