# Changelog for oda_reader

## 1.1.2 (2025-04-16)
- Extends caching to bulk downloaded files.
- Other minor tweaks to how caching works.

## 1.1.1 (2025-04-16)
- Manages an issue created by the OECD when they are about to release new data. In that case
certain dataflows return `NoRecordsFound`, even though the query is valid for lower dataflows. 
This version of `oda_reader` defends against that.


## 1.1.0 (2025-04-9)
- Introduces configurable and persistent caching via `joblib`. By default, the reader
  will keep a cache on disk (up to 1GB, for up to 7 days). This is to better manage the
  OECD's aggressive rate throttling (20 requests per minute). Entries older than 7
  days are automatically cleared and `clear_cache` can be used to manually clear it.


## 1.0.6 (2025-02-15)
- Improves warnings for duplicates on the multisystem dataset

## 1.0.5 (2025-02-15)
- Improves automatic handling of dataflows.

## 1.0.4 (2025-02-15)
- Improves automatic handling of DE CRS data.

## 1.0.3 (2025-02-15)
- Improves automatic handling of DE CRS data.

## 1.0.2 (2025-01-06)
- Improves automatic handling of dataflows when a dataflow exists but has no data.

## 1.0.1 (2025-01-06)
- Improves automatic handling of dataflows

## 1.0.0 (2024-10-06)
- Major release marking version 1.0.0.
- Adds API support for the CRS and Multisystem datasets.
- Adds support for bulk downloading of the CRS and Multisystem datasets in parquet format.
- Improves filtering options for all datasets (DAC1, DAC2a, CRS, Multisystem).
- Enhanced performance and stability for large dataset queries and downloads.
- General codebase improvements and documentation updates.

## 0.2.3 (2024-09-16)
- Fixes an error returned when making an API call to DAC1 without specifying the dataflow version.
- An option to specify the dataflow version is now provided
- This release pins dac1 dataflow to 1.2

## 0.2.2 (2024-06-28)
- The schema provided by the OECD identifies the EU institutions under a code with no data. This update matches the right new code to the old 918.
- The schema provided by the OECD does not correctly identify the donor code for DAC EU countries + EU Institutions. This update correctly matches it.

## 0.2.1 (2024-05-05)
- Allows for direct imports of `download_dac1`, `download_dac2a` and `QueryBuilder` as
`from oda_reader import download_dac1, download_dac2a, QueryBuilder`.


## 0.2.0 (2024-05-05)
- Fixes a bug with `download_dac2a` which meant filters were not applied properly
and the wrong schema (dac1) was loaded.
- Added a new method to the query builder to generate a dac2a filter expression.

## 0.1.0 (2024-05-05)
- Initial release. It includes a basic implementation of an API call for DAC1 and DAC2.
- This release includes tools to translate the API response into the old .Stat schema.