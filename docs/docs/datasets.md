# Datasets Overview

ODA Reader provides access to five datasets covering official development assistance (ODA), other official flows (OOF), and development finance. Each dataset serves different analytical needs.

## Quick Reference

| Dataset | What It Contains | Use When |
|---------|------------------|----------|
| **DAC1** | Aggregate flows by donor | Analyzing overall ODA trends, donor performance |
| **DAC2a** | Bilateral flows by donor-recipient | Recipient-level analysis |
| **CRS** | Project-level microdata | Sector analysis, project details, activity-level data |
| **Multisystem** | Multilateral system usage | Analyzing multilateral channels and contributions |
| **AidData** | Chinese development finance | Chinese aid flows |

## DAC1: Aggregate Flows

**What it contains**: Total ODA and OOF by donor, aggregated across all recipients and sectors. This is the highest-level view of development assistance.

**Key dimensions**:

- Donor (bilateral donors and multilateral organizations)
- Measure type (ODA, OOF, grants, loans, etc.)
- Flow type (commitments, disbursements, grant equivalents)
- Price base (current or constant prices)
- Unit measure (USD millions, national currency, etc.)

**Use when**:

- You need donor-level totals
- Analyzing overall ODA trends over time
- Comparing donor performance
- Working with high-level aggregates

**Example**:

```python
from oda_reader import download_dac1

# Get all DAC1 data for 2020-2022
data = download_dac1(start_year=2020, end_year=2022)

# Filter for ODA disbursements in constant prices
oda_constant = download_dac1(
    start_year=2020,
    end_year=2022,
    filters={
        "measure": "1010",  # Net ODA
        "flow_type": "1140",  # Disbursements
        "price_base": "Q"  # Constant prices
    }
)
```

[Read more about filtering →](filtering.md)

## DAC2a: Bilateral Flows by Recipient

**What it contains**: Bilateral ODA and OOF flows broken down by both donor and recipient country. Shows who gives to whom.

**Key dimensions**:

- Donor (bilateral donors)
- Recipient (receiving countries and regions)
- Measure type (bilateral ODA, imputed multilateral, etc.)
- Price base (current or constant)

**Use when**:

- Analyzing flows to specific recipient countries
- Understanding bilateral relationships
- Studying geographic distribution of aid
- Comparing different donors to the same recipient

**Example**:

```python
from oda_reader import download_dac2a

# Get flows to Sub-Saharan Africa from all donors
africa_flows = download_dac2a(
    start_year=2020,
    end_year=2022,
    filters={"recipient": "289"}  # Sub-Saharan Africa (regional code)
)

# Get flows from Germany to East African countries
germany_eastafrica = download_dac2a(
    start_year=2022,
    end_year=2022,
    filters={
        "donor": "DEU",
        "recipient": ["KEN", "TZA", "UGA", "RWA"]
    }
)
```

## CRS: Creditor Reporting System (Project-Level Microdata)

**What it contains**: Individual project and activity-level data with detailed information about each development assistance activity. This is the most granular dataset.

**Key dimensions**:

- Donor
- Recipient
- Sector (purpose codes at various levels of detail)
- Channel (implementing organization type)
- Modality (grant, loan, equity, etc.)
- Flow type
- Microdata flag (True for project-level, False for semi-aggregates)

**Use when**:

- You need project-level details (descriptions, amounts, sectors)
- Analyzing sector-specific flows
- Understanding implementation channels
- Detailed activity-level analysis

**Important**: CRS defaults to **microdata** (project-level). For semi-aggregates matching the online Data Explorer view, set `microdata: False` in filters.

**Example (microdata)**:

```python
from oda_reader import download_crs

# Get all health sector projects from Canada
health_projects = download_crs(
    start_year=2022,
    end_year=2022,
    filters={
        "donor": "CAN",
        "sector": "120"  # Health sector (3-digit code)
    }
)

# Each row is a project with description, amount, dates, etc.
```

**Example (semi-aggregates)**:

```python
# Get semi-aggregated CRS data (matches online Data Explorer)
semi_agg = download_crs(
    start_year=2022,
    end_year=2022,
    filters={
        "donor": "USA",
        "recipient": "NGA",
        "microdata": False,
        "channel": "_T",  # Total across all channels
        "modality": "_T"  # Total across all modalities
    }
)
```

**Performance note**: The CRS API is slow for large queries. Consider using [bulk downloads](bulk-downloads.md) for full dataset access.

## Multisystem: Members' Use of the Multilateral System

**What it contains**: Data on how DAC members use the multilateral aid system, including core contributions to multilateral organizations and earmarked funding.

**Key dimensions**:

- Donor
- Recipient (multilateral organizations)
- Channel (specific multilateral organizations)
- Flow type (commitments, disbursements)
- Measure type

**Use when**:

- Analyzing multilateral contributions
- Understanding core vs. earmarked funding
- Studying specific multilateral channels (World Bank, UN agencies, etc.)

**Example**:

```python
from oda_reader import download_multisystem

# Get all multilateral contributions from France
france_multilateral = download_multisystem(
    start_year=2020,
    end_year=2022,
    filters={"donor": "FRA"}
)

# Get contributions to World Bank IDA
ida_contributions = download_multisystem(
    start_year=2020,
    end_year=2022,
    filters={"channel": "44002"}  # IDA
)
```

**Performance note**: Like CRS, Multisystem API can be slow. [Bulk download](bulk-downloads.md) is available for the full dataset.

## AidData: Chinese Development Finance

**What it contains**: Project-level data on Chinese development finance activities, compiled by AidData. Covers official finance from China that may not be reported to the OECD.

**Key dimensions**:

- Commitment year
- Recipient country
- Sector
- Project descriptions
- Flow amounts and types

**Use when**:

- Analyzing Chinese development finance
- Comparing DAC donors with China

**Example**:

```python
from oda_reader import download_aiddata

# Get all AidData records for 2015-2020
chinese_aid = download_aiddata(start_year=2015, end_year=2020)

# AidData is downloaded as bulk file, filtered by year after download
```

**Note**: AidData comes from Excel files from the Aid Data website, not the OECD API. It uses a different schema than DAC datasets.

## Discovering Available Filters

Each dataset has different dimensions you can filter by. Use `get_available_filters()` to see what's available:

```python
from oda_reader import get_available_filters

# See available filters for each dataset
dac1_filters = get_available_filters("dac1")
dac2a_filters = get_available_filters("dac2a")
crs_filters = get_available_filters("crs")
multisystem_filters = get_available_filters("multisystem")
```

[Learn more about filtering →](filtering.md)

## Next Steps

- **[Filtering Data](filtering.md)** - Build complex queries with multiple dimensions
- **[Bulk Downloads](bulk-downloads.md)** - Download full CRS, Multisystem, or AidData efficiently
- **[Schema Translation](schema-translation.md)** - Understand API vs. .Stat schema codes
