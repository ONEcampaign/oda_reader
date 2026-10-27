---
description: Read the OECD Climate-Related Development Finance (CRDF) workbooks from the provider or recipient perspective, with one column schema across every known layout, the price year and the file each row came from.
---

# Climate-related development finance

The OECD publishes the Climate-Related Development Finance (CRDF) dataset as Excel workbooks at [webfs.oecd.org/climate](https://webfs.oecd.org/climate/), one folder per perspective. ODA Reader reads them over plain HTTP, finds file names in the live folder listing, and maps every known header layout to one set of column names.

## Read a range of years

```python
from oda_reader import download_crdf

provider = download_crdf("provider", start_year=2015, end_year=2024)
recipient = download_crdf("recipient", start_year=2021)
```

The perspective is `"provider"` or `"recipient"`. Without `start_year` or `end_year` the reader returns every year the folder holds: 2013 to 2024 for providers and 2000 to 2024 for recipients in the October 2026 listing.

For each year the reader picks the narrowest workbook that holds it. The 2015 recipient year comes from `CRDF-RP_2014-2015.xlsx`, and the all-years workbooks are read only when no narrower file holds a year.

## Columns

Amounts are commitments in USD thousand, as published. Each amount comes in current prices (`*_current_kusd`) and constant prices (`*_constant_kusd`). Since the June 2026 republication every workbook uses 2024 prices.

Five columns describe where each row came from:

| Column              | Meaning                                                                                                              |
| ------------------- | -------------------------------------------------------------------------------------------------------------------- |
| `price_year`        | The price year of the `*_constant_kusd` columns, read from the headers                                               |
| `vintage`           | The workbook's last-modified date, which matches the folder's `Last-Modified` header                                 |
| `source_file`       | The file name, for example `CRDF-PP_2024.xlsx`                                                                       |
| `file_version`      | The file host's validator recorded when the cached copy was downloaded, or missing for a local file or no validator  |
| `file_version_kind` | `etag` or `last_modified`, naming the header `file_version` came from                                                |

The current recipient workbooks carry `climate_share_pct`, the climate share of the underlying commitment from 0 to 100. Archived recipient workbooks carry `underlying_commitment_share` instead, which runs from 0 to 1. Archived workbooks also carry `provider_short_name`, `concessionality`, `financial_instrument` and `coal_related_financing`, and lack the flow type and the environment, biodiversity and desertification markers.

Pass `pre_process=False` to keep the published headers and the types Excel gives. The five provenance columns are added either way.

## Read an archived copy

Older releases differ in layout. Read a copy you hold with `file=`:

```python
df = download_crdf("provider", file="archive/DP_2022.xlsx")
```

The file must use a known layout and the perspective you name.

## Unknown layouts

The OECD maintains the folder by hand and has renamed files and columns before. The header row of every data sheet must match one of the layouts in `oda_reader/schemas/mappings/crdf_schema.json`, after the price year is abstracted. Anything else raises `CRDFSchemaError`, which names the closest known layout and lists the headers it does not map (`unmapped`) and those it lacks (`missing`):

```python
from oda_reader import CRDFSchemaError, download_crdf

try:
    df = download_crdf("recipient")
except CRDFSchemaError as error:
    print(error.layout, error.unmapped, error.missing)
```

The error is also raised when a sheet carries two price years, when a workbook belongs to the other perspective, or when a column holds values that do not fit its type.

| Layout           | Files                                                                    |
| ---------------- | ------------------------------------------------------------------------ |
| `provider_2026`  | Provider workbooks republished from June 2026, 40 columns                |
| `recipient_2026` | Recipient workbooks republished from June 2026, 40 columns               |
| `provider_2021`  | Archived `DonorPerspective` workbooks published 2021 to 2024, 38 columns |
| `recipient_2023` | Archived recipient workbooks published 2023 and 2024, 38 columns         |
| `recipient_2025` | The archived recipient workbook published in 2025, 38 columns            |

## List and download files

`list_crdf_files()` returns the folder listing, one row per workbook, with its perspective, the years in its name, its size and URL:

```python
from oda_reader import download_crdf_file, list_crdf_files

files = list_crdf_files()
download_crdf_file("Imputed_multilateral_shares.xlsx", "./data/")
```

`download_crdf_file` saves a workbook into a directory, or to a full path, and returns where it went. It reads top-level files such as the imputed multilateral shares too.

## Caching

Workbooks are cached in the [bulk file cache](caching.md#bulk-file-cache) beside the other bulk files. Each call asks the host for the file's ETag, or its `Last-Modified` date when it sends no ETag, with a one-byte request, and downloads the file again only when that value has changed or the copy is older than 90 days. When the request fails, the cached copy is read and keeps the `file_version` recorded with it. The folder listing is fetched fresh on every call.
