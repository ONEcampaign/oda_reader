# Datasets Overview

ODA Reader provides access to seven datasets covering official development assistance (ODA), other official flows (OOF), and development finance. Each dataset serves different analytical needs.

## Quick Reference

| Dataset         | What It Contains                   | Use When                                               |
| --------------- | ---------------------------------- | ------------------------------------------------------ |
| **DAC1**        | Aggregate flows by donor           | Analyzing overall ODA trends, donor performance        |
| **DAC2a**       | Bilateral flows by donor-recipient | Recipient-level analysis                               |
| **DAC2b**       | Bilateral OOF and export credits   | Non-concessional flows and export credits by recipient |
| **CRS**         | Project-level microdata            | Sector analysis, project details, activity-level data  |
| **CPA**         | Country Programmable Aid           | The share of aid donors programme at country level     |
| **Multisystem** | Multilateral system usage          | Analyzing multilateral channels and contributions      |
| **AidData**     | Chinese development finance        | Chinese aid flows                                      |

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

**Bulk download**: The full dataset is available as a single file via `bulk_download_dac1()`. See [Bulk Downloads](bulk-downloads.md#dac1-bulk-download) for details.

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

## DAC2b: Other Official Flows and Export Credits

**What it contains**: Bilateral OOF and export credits broken down by donor and recipient country, the non-concessional counterpart to DAC2a's ODA flows. It shares DAC2a's dimensions and dimension order (`DSD_DAC2` covers both tables), so `build_dac2_filter`, `get_available_filters`, and the schema translation all work the same way.

**Key dimensions**: Donor, recipient, measure type, and price base, the same as DAC2a.

**Measure codes** are OOF/export-credit aggregates, already in .Stat numbering:

| Code   | Measure                              |
| ------ | ------------------------------------ |
| `2201` | OOF grants                           |
| `2204` | OOF loans, disbursements             |
| `2205` | OOF loans, repayments                |
| `2217` | OOF equity investment                |
| `2250` | Export credits, total net            |
| `2255` | Net OOF                              |
| `2292` | Export credits, gross                |
| `2293` | Export credits, repayments           |
| `2295` | Offsetting entries for debt relief   |
| `2296` | Official non-concessional flows, net |
| `2297` | Interest received on OOF             |
| `2972` | OOF, gross                           |

**Use when**:

- Analyzing non-concessional official flows to specific recipients
- Tracking export credit exposure by donor or recipient
- Distinguishing OOF from ODA in a donor-recipient breakdown

**Example**:

```python
from oda_reader import download_dac2b

# Get OOF and export credits to Sub-Saharan Africa from all donors
africa_oof = download_dac2b(
    start_year=2020,
    end_year=2022,
    filters={"recipient": "289"}  # Sub-Saharan Africa (regional code)
)

# Get export credits, gross, from Germany to East African countries
germany_eastafrica = download_dac2b(
    start_year=2022,
    end_year=2022,
    filters={
        "donor": "DEU",
        "recipient": ["KEN", "TZA", "UGA", "RWA"],
        "measure": "2292"  # Export credits, gross
    }
)
```

**Bulk download**: The full dataset is available as a single file via `bulk_download_dac2b()`. See [Bulk Downloads](bulk-downloads.md#dac2b-bulk-download) for details. The bulk file carries a `PART` dimension and legacy `AIDTYPE` codes not present in the API response above.

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

## CPA: Country Programmable Aid

**What it contains**: The share of bilateral ODA that donors programme for individual partner countries. CPA strips out flows a partner country has no say over — debt relief, humanitarian aid, in-donor refugee and student costs, administrative costs, and other non-programmable items. The OECD publishes it as a separate dataflow (`DSD_CPA@DF_CRS_CPA`) derived from the CRS, so it shares the CRS schema, dimensions, and filter set.

**Key dimensions**: Same as the CRS — donor, recipient, sector, channel, modality, flow type, and the microdata flag.

**Use when**:

- You want the country-programmable slice of aid rather than total bilateral ODA
- Comparing how much of each donor's aid is programmable at country level
- Tracking programmable aid to specific recipients or sectors over time

**Important**: Like the CRS, `download_cpa` defaults to **microdata** (`microdata=True`, i.e. `MD_DIM=DD`), returning project-level records. There is no grant-equivalent dataflow for CPA, so `as_grant_equivalent` is not available.

**Example**:

```python
from oda_reader import download_cpa

# Get all CPA records for 2022
cpa = download_cpa(start_year=2022, end_year=2022)

# Country-programmable aid from the United States to Nigeria
us_nga = download_cpa(
    start_year=2022,
    end_year=2022,
    filters={"donor": "USA", "recipient": "NGA"}
)
```

The available filters match the CRS and can be listed with `get_available_filters("cpa")`.

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
dac2b_filters = get_available_filters("dac2b")
crs_filters = get_available_filters("crs")
cpa_filters = get_available_filters("cpa")
multisystem_filters = get_available_filters("multisystem")
```

[Learn more about filtering →](filtering.md)

## Next Steps

- **[Filtering Data](filtering.md)** - Build complex queries with multiple dimensions
- **[Bulk Downloads](bulk-downloads.md)** - Download full CRS, Multisystem, or AidData efficiently
- **[Schema Translation](schema-translation.md)** - Understand API vs. .Stat schema codes
