# Changelog for oda_reader


## 0.2.0 (2024-05-05)
- Fixes a bug with `download_dac2a` which meant filters were not applied properly
and the wrong schema (dac1) was loaded.
- Added a new method to the query builder to generate a dac2a filter expression.

## 0.1.0 (2024-05-05)
- Initial release. It includes a basic implementation of an API call for DAC1 and DAC2.
- This release includes tools to translate the API response into the old .Stat schema.