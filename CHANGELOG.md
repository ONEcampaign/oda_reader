# Changelog for oda_reader

## 1.7.0 (2026-07-28)

- Adds `oda_reader.codelists`, a new public surface for fetching the OECD DAC area codelists
  (providers and recipients) directly, without going through a bulk download. Import it
  explicitly — `from oda_reader.codelists import fetch_codelists` — since `oda_reader.codelists`
  is not re-exported from `oda_reader`.
  `fetch_codelists()` returns a `CodelistSnapshot` whose `frame` has one row per
  `(codelist_id, code, activation_date)`; `parse_codelists()` builds the same snapshot from bytes
  already in hand, with no network access, so a pipeline's own stored payloads keep working when
  the OECD page breaks. Five typed exceptions (`CodelistError` and its four subclasses) are
  importable from `oda_reader` directly. Only codelists `"5"` (providers) and `"13"` (recipients)
  are supported; see the [Codelists docs](https://github.com/ONEcampaign/oda_reader/blob/main/docs/docs/codelists.md)
  for the row grain and the reconcile workflow, and the
  [Codelists Reference](https://github.com/ONEcampaign/oda_reader/blob/main/docs/docs/codelists-reference.md)
  for the frame contract and the exception hierarchy.
  **Documented-provisional** until the earlier of a future `1.x` release or **2026-12-31**:
  breaking changes to the frame's shape are still possible during this window and will be
  announced here rather than held to the usual deprecation cadence.
- Adds `reconcile()` to `oda_reader.codelists`, a pure function that merges a fresh
  `CodelistSnapshot` into a previous codelist table and returns a `Reconciliation`. It never
  deletes a row. A code OECD stops listing comes back with
  `presence="retired"` and its last known values intact, instead of vanishing the moment a
  `CREATE OR REPLACE` runs — the only way to lose a code is to not pass `previous`. Retirement is
  scoped to the codelists actually requested, a code that reappears is a change rather than a new
  addition, and `previous` is coerced to the frame's contract dtypes on entry so a DuckDB
  `.df()` result works without manual casting. `Reconciliation.table`, `.added`, `.retired` and
  `.changed` are documented in the [Codelists docs](https://github.com/ONEcampaign/oda_reader/blob/main/docs/docs/codelists-reference.md#reconcile).
  `reconcile` supports the area contract only; it raises `CodelistValidationError` on a
  category- or agency-contract snapshot rather than silently mis-keying it.
- Adds `fetch_code_categories()` / `parse_code_categories()` to `oda_reader.codelists`, the
  category contract: the same live-fetch/no-network-escape-hatch pair as `fetch_codelists` /
  `parse_codelists`, over the 23 flat non-area OECD DAC codelists (purpose codes, channels of
  delivery, markers, and the rest — everything except Provider, Recipient, and Provider agency,
  which is a different entity keyed additionally on `donor_code` and has its own contract, below).
  Returns the same `CodelistSnapshot` type with an eleven-column frame keyed on
  `(codelist_id, code, activation_date, crs, tossd)` — one row per reporting standard, because
  four of these codelists (Purpose code, Channel of delivery, Type of finance, Co-operation
  modality) carry the same code with a different meaning under CRS versus TOSSD reporting.
  `codelist_ids` has no default, unlike `fetch_codelists` — there's no sensible default across 23
  codelists, and defaulting to all of them would make a ~900 KB fetch the accidental behaviour of
  a bare call. See the [category contract docs](https://github.com/ONEcampaign/oda_reader/blob/main/docs/docs/codelists-reference.md#category-codelists)
  for the frame contract, the five-column key, `parent_code` (the CRS sector hierarchy) and
  `dac_reference` (opaque provenance text, not a controlled vocabulary). Ships under the same
  **documented-provisional** window as the area contract.
- Adds `fetch_provider_agencies()` / `parse_provider_agencies()`, covering codelist `"16"`
  (Provider agency) — the last OECD codelist the package didn't reach. Its frame has twelve
  columns, one row per `(codelist_id, donor_code, code, activation_date)`. **`donor_code` is part
  of the key because an agency code is only meaningful within its donor**: code `1` is the Federal
  Ministry of Finance under one donor and the Ministry of Foreign Affairs under another, and 99 of
  the 110 distinct codes carry more than one label across donors — code `1` alone carries 185.
  Joining agency rows on `code` alone silently merges unrelated agencies; join on `donor_code` and
  `code` together. Beyond the code-to-name map that `download_crs` already returns inline, the
  frame carries `agency_type_code`/`agency_type`, `acronym` and `used_in_cpa`. Agency rows carry no
  activation date today, so that column is present but empty. `fetch_provider_agencies` takes no
  `codelist_ids` argument — there is exactly one legal value — and `SUPPORTED_AGENCY_IDS` is
  exported alongside its two siblings. `reconcile()` still accepts area snapshots only, and now
  rejects an agency snapshot with a message naming the contract. Ships under the same
  **documented-provisional** window as the other two contracts.
- `CodelistSnapshot` gains a required `contract` field — `"area"`, `"category"` or `"agency"` —
  recording which fetch/parse pair built it, so code that receives a snapshot second-hand can tell
  the three apart without inspecting `frame.columns`. `reconcile()` checks `contract` first and
  keeps the column-tuple comparison as a second check, for a snapshot rebuilt by hand from stored
  `raw` where the two can disagree. Code that constructs a `CodelistSnapshot` directly must now
  pass `contract=`.
- `parse_codelists()`, `parse_code_categories()` and `parse_provider_agencies()` now reject an
  empty `raw` mapping with `CodelistValidationError` instead of returning a valid-looking snapshot
  with zero rows. A replay whose stored payload was missing or unreadable used to succeed silently,
  covering zero codelists — and since retirement is scoped to the codelists in the snapshot,
  `reconcile()` then reported `is_unchanged` on a run that had read nothing at all. A single
  codelist returning zero rows was already rejected; this closes the same hole one level up.
- Label selection now prefers a narrative entry explicitly tagged `en` (either `xml:lang` or
  `_xml:lang`, case-insensitively, including regional forms like `en-GB`) over the untagged-means-
  English heuristic, falling back to that heuristic for the shape OECD publishes today. Previously
  an explicitly tagged English entry was skipped as "not untagged" and the French text was returned
  in its place. No shipped label changes — no OECD row currently tags its English narrative — but
  the contract no longer depends on that staying true.
- Fixes `codelist_ids` validation on both `fetch_codelists` and `fetch_code_categories`: a bare
  string (e.g. `codelist_ids="10"`) used to be iterated character-by-character, silently
  requesting codelists `"1"` and `"0"` instead of `"10"` — wrong data with no error raised. A bare
  string is now rejected with `CodelistValidationError` before any request is made. A non-string id
  (e.g. `10` instead of `"10"`) now raises with a message telling you to pass it as a string,
  instead of a bare "unsupported" error. `codelist_ids=["16"]` is now rejected by name on both
  functions, pointing at `fetch_provider_agencies` / `parse_provider_agencies` — on
  `fetch_codelists` it previously fell through to the bare "unsupported" message, and on
  `fetch_code_categories` it reported `"16"` as out of scope entirely, which is no longer true.
- `SUPPORTED_CODELIST_IDS`, `SUPPORTED_CATEGORY_IDS`, `SUPPORTED_AGENCY_IDS`, `STATUS_DOMAIN`,
  `PRESENCE_DOMAIN` and `LINEAGE_COLUMNS` are now public exports of `oda_reader.codelists`, so a
  pipeline can validate ids or reconciliation columns against the same constants the library uses
  internally instead of hardcoding them.
- Every supported codelist id is now named, not just numbered. Public docstrings and the
  [Codelists docs](https://github.com/ONEcampaign/oda_reader/blob/main/docs/docs/codelists-reference.md#fetch_code_categories)
  list all 23 category ids by OECD's own dropdown label, the area contract's `"5"` (Provider) and
  `"13"` (Recipient) are named explicitly, and the
  [Codelist ID Reference](https://github.com/ONEcampaign/oda_reader/blob/main/docs/docs/codelist-ids.md)
  maps every id to the contract that covers it. With the agency contract added, every OECD codelist
  except `"0"` ("All codes list", which returns all 26 blocks in one response) is now reachable.
- Refreshes the committed DAC area-code mappings (`dac1_codes_area.json`, `dac2_codes_area.json`)
  from the live OECD codelist app. The previous source — `stats.oecd.org` FileView GUID URLs, parsed
  by positional XML index — now returns 404, so it is replaced by
  `scripts/data_maintenance/refresh_dac_codelists.py`, which performs the same ASPX handshake the
  `codelists` package uses and merges additively into the committed crosswalks: codes the live app
  no longer serves are preserved, and the live value wins on a conflict. **This changes translated
  output**: DAC1 gains 25 codes (416 → 441), DAC2a gains 13 (430 → 443), and donor `918` (EU
  Institutions) now maps to `4EU001` rather than `4EU003`, so `download_dac1` and `download_dac2a`
  callers using the `.stat` schema will see the new code. A weekly `codelist-drift` workflow opens a
  PR when the committed mappings fall out of date with the live source.
- Adds `--audit` to `scripts/data_maintenance/refresh_dac_codelists.py`, which prints the
  withdrawn-record conflict/orphan/new-candidate findings behind the committed area-code mappings
  and writes nothing; mutually exclusive with `--write`.
- Adds `download_cpa()` for the OECD Country Programmable Aid (CPA) dataset
  (`DSD_CPA@DF_CRS_CPA`), sourced directly from the OECD SDMX API. CPA reuses the CRS filter
  and `.stat` schema; `get_available_filters("cpa")` is supported. Per-year bulk download is
  deferred because the OECD bulk files are currently malformed (tracked in
  [#39](https://github.com/ONEcampaign/oda_reader/issues/39)).
- Project maintenance: adopted the [`bblocks-projects`](https://github.com/ONEcampaign/bblocks-projects)
  template standard so the repo is now managed (`bblocks-projects update` / `doctor` work via
  `.copier-answers.yml`). Adds the `ty` type checker (enforced in CI and pre-commit) and full
  public-API type annotations, expands ruff rules (annotations, perflint, eradicate), refreshes
  pre-commit hooks (codespell, actionlint, mdformat, ty), and adds a `py.typed` marker so type
  information ships with the wheel.
- **Minimum supported Python is now 3.11** (was 3.10). Python 3.10 reaches end-of-life in
  October 2026.
- CI release now uses PyPI trusted publishing (OIDC) instead of an API token.
- **Minimum supported `pyarrow` is now 23.0.1** (was 14.0.0), clearing GHSA-6r8h-32rm-w2gh — a
  potential use-after-free when reading an IPC file with pre-buffering, which affects every
  release from 15.0.0 up to 23.0.1. Raising the floor rather than only re-locking means an install
  of `oda_reader` can no longer resolve a vulnerable `pyarrow`. `pyarrow` 23 requires Python 3.10+,
  which the package's own 3.11 floor already satisfies.
- Bumps three transitive dependencies past published advisories and pins their floors in
  `constraint-dependencies` so a future re-lock cannot regress: `urllib3` to 2.7.0 (decompression-
  bomb safeguards bypassed in parts of the streaming API; sensitive headers forwarded across
  origins in proxied low-level redirects), `idna` to 3.15+ (crafted inputs bypassing the
  CVE-2024-3651 fix), and `pymdown-extensions` to 11.0.0+ (two `b64`/`snippets` path traversals
  allowing reads outside `base_path`). Only `pymdown-extensions` is a docs-group dependency; it
  does not reach the installed package.

## 1.6.0 (2026-04-28)

- Adds `use_raw_cache=False` to `bulk_download_crs`, `download_crs_file`, `bulk_download_dac2a`
  and `bulk_download_multisystem` for the cases where you want to bypass the bulk cache and
  re-download fresh on every call. Caching remains on by default.
- Adds a typed `BulkPayloadCorruptError` (importable from `oda_reader`) for the rare case
  where a freshly downloaded zip arrives corrupt. The corrupt entry is removed before the
  exception is raised, so the next call cleanly re-downloads. The error message tells you
  what to do next.
- Strengthens corruption detection by validating freshly downloaded zips end-to-end (full
  member CRC check), not just the central directory. Cached files are trusted on hit so
  this doesn't slow normal use.
- The bulk cache now self-maintains: it keeps the two most recent downloads per dataset
  and evicts older ones, and sweeps stale temp files left behind by interrupted downloads
  (older than 24h) on startup. No more manually clearing the cache directory to free space.
- Temp-file naming now includes the hostname alongside the PID, preventing collisions when
  the cache directory lives on a shared / NFS mount used by multiple machines.
- `clear_cache`, `set_cache_dir`, `enable_cache` and `disable_cache` now emit a
  `DeprecationWarning` for users who also import `oda_data`, pointing at the umbrella
  `oda_data.cache.*` API. Standalone `oda_reader` users see no warning. The shims continue
  to work through the `1.x` series and will be removed in `2.0`.
- Cache directory is now versioned by the installed package version (via `importlib.metadata`) rather than a hardcoded string, so upgrades automatically invalidate old caches that may contain partial or corrupt downloads from prior versions.
- Bulk-download cache writes are now atomic: downloads stream into a sibling temp file and are only renamed over the destination on success, so partial downloads no longer pollute the cache on interruption or error.
- On `BadZipFile`, the corrupt cached archive is removed so the next call cleanly re-downloads instead of looping on the same poisoned entry.
- Cached archives are validated with `zipfile.is_zipfile` before reuse; a corrupt entry is removed and re-downloaded transparently.

## 1.5.1 (2026-04-15)

- Adds support for Deflate64-compressed ZIP files in bulk downloads. The OECD switched the full CRS bulk file to Deflate64 compression, which Python's standard library does not support. This release patches `zipfile` at runtime using the `inflate64` library to handle Deflate64 transparently.
- Adds `inflate64` as a dependency.

## 1.5.0 (2026-04-09)

- Replaces blind version-decrement fallback with authoritative SDMX metadata endpoint lookup for all datasets.
- Adds `clear_version_cache()` to the public API for forcing fresh version discovery mid-session.

## 1.4.3 (2026-04-09)

- Fixes DAC1 query filter dimension order to match the current DSD schema, which added SECTOR at position 2.
- Bumps DAC1 dataflow version from 1.7 to 1.8.

## 1.4.2 (2026-01-23)

- Updates DAC1 dataflow version from 1.5 to 1.7.
- Updates DAC2a dataflow version from 1.6 to 1.4.

## 1.4.1 (2025-12-19)

- Extends bulk download auto-detection to support `.csv` files in addition to `.txt` files.

## 1.4.0 (2025-12-19)

- Adds `bulk_download_dac2a()` function for bulk downloading the full DAC2A dataset.
- Auto-detects file types (parquet or txt) in bulk downloads, removing the need for the `is_txt` parameter.
- Auto-detects CSV delimiters (comma, pipe, tab, semicolon) when reading txt files from bulk downloads.
- Deprecates the `is_txt` parameter in `bulk_download_parquet()`. The parameter is still accepted for backward compatibility but emits a deprecation warning and will be removed in a future major release.
- Adds pytest and pytest-mock to dev dependencies for improved testing support.

## 1.3.5 (2025-12-19)

- Fixes `_get_dataflow_version()` to gracefully handle URLs without a version pattern instead of crashing.

## 1.3.4 (2025-12-19)

- Improves robustness of dataflow version fallback logic. The API error detection now checks response content regardless of HTTP status code, handling cases where error messages like "Could not find Dataflow" are returned with various status codes.

## 1.3.3 (2025-12-19)

- Reverts DAC1 dataflow version from 1.6 to 1.5 to ensure compatibility with published data.

## 1.3.2 (2025-12-19)

- Updates bulk file dataflow version to 1.6 to match OECD's latest schema.

## 1.3.1 (2025-06-27)

- Improves cache management for very large files. Introduces tests and improved documentation

## 1.3.0 (2025-06-16)

- Improves cache management.

## 1.2.2 (2025-06-16)

- Fixes a bug with AidData where passing None for end year would raise
  a type error.

## 1.2.1 (2025-06-10)

- Introduces rate limiting (20 requests per minute).This is to better manage the OECD's aggressive rate throttling (20 requests per minute).

## 1.2.0 (2025-06-05)

- Introduces importer tools for the AidData's Global Chinese development finance dataset.
- Introduces functionality to stream bulk files to avoid loading too much data to memory

## 1.1.5 (2025-05-15)

- The OECD has unexpectedly (and quietly) changed the naming convention
  for the bulk Multisystem data. This is a small fix to address that.

## 1.1.4 (2025-04-22)

- fix small cache bug

## 1.1.3 (2025-04-22)

- Small caching improvements

## 1.1.2 (2025-04-22)

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
