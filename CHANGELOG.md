# Changelog for oda_reader

## 1.11.2 (2026-09-15)

- **DAC1, DAC2a and DAC2b bulk downloads detect republished files again on hosts that reject
  HEAD.** The publication-version token now comes from a one-byte ranged GET against the
  resolved URL, reading `ETag` (falling back to `Last-Modified`). A failed token lookup logs
  one warning per URL per process naming the HTTP status or the Cloudflare Ray ID, and cache
  invalidation falls back to the 30-day TTL.

## 1.11.1 (2026-09-09)

- **Reports Cloudflare challenges on OECD bulk downloads.** `BulkDownloadChallengeError` carries
  the HTTP status, URL, a bounded response preview, and the Cloudflare Ray ID. It remains a
  subclass of `BulkDownloadHTTPError` and `ConnectionError`. Challenges and other 403 responses
  raise after the first response, while a 404 triggers one metadata refresh. Response previews
  are capped at 1 KiB, and a failed preview read preserves the original diagnosis.
- **Adds an opt-in live check for stable bulk URLs.** The check uses a one-byte Range GET. The
  bulk-download guide also explains filtered SDMX API fallbacks and their schema differences,
  including the DAC1 API's omission of the bulk-file `PART` dimension.
- **Raises the docs-only `mkdocs-material` dependency to 9.7.7.** This version fixes
  [GHSA-xvg9-69gf-fjrf](https://github.com/squidfunk/mkdocs-material/security/advisories/GHSA-xvg9-69gf-fjrf),
  a DOM XSS in the optional search-suggestion feature. ODA Reader uses the default search
  configuration. The new dependency floor also removes the vulnerable version from `uv.lock`.

## 1.11.0 (2026-09-09)

- **Adds `download_dac2b()` and `bulk_download_dac2b()`** for DAC2b (`DSD_DAC2@DF_DAC2B`, version
  1.7), Other Official Flows and export credits. DAC2b shares DAC2a's dimensions, dimension
  order, and CSV schema (both dataflows live under `DSD_DAC2`), so it reuses the same filter
  builder and schema translation. Only the `measure` codes differ: OOF and export-credit
  aggregates such as `2204` OOF loans disbursements and `2292` export credits gross. Unlike
  DAC2a's, DAC2b's API `MEASURE` codes are not already in .Stat numbering. On the default path
  (`dotstat_codes=True`), `download_dac2b()` now converts the returned `aidtype_code` from the
  API's numbering to .Stat numbering (`2204` → `204`, `2292` → `292`, and so on for all 13 codes),
  via an explicit mapping table, matching the codes `bulk_download_dac2b()`'s bulk file carries.
  `filters={"measure": ...}` still takes the API code, since filters are sent before conversion
  runs. An unmapped code raises `ValueError` rather than silently becoming `NaN`.
- **Corrected the Sub-Saharan Africa examples for both DAC2a and DAC2b.** `docs/docs/datasets.md`
  passed the .Stat area code `289` as a `recipient` filter. Filters reach the API before any code
  conversion, so the query raised rather than returning the region, and `289` denotes the
  unspecified residual rather than the region in any case. Both now pass the API code `F6`.
- **The DAC2b bulk file uses the legacy .Stat schema, not the API's 26-column layout.** OECD
  publishes it as one zipped CSV, 20.6 MB compressed and 2,281,210 rows, expanding to 459.8 MB.
  Its 14 columns pair a code with a label for each dimension (`RECIPIENT`/`Recipient`,
  `DONOR`/`Donor`, `PART`/`Part`, `AIDTYPE`/`Aid type`, `DATATYPE`/`Amount type`, `TIME`/`Year`,
  plus `Value` and `Flags`), the same layout `bulk_download_dac1()` and `bulk_download_dac2a()`
  use. The bulk file carries a `PART` dimension the API dataflow does not expose, and its
  `AIDTYPE` codes are legacy .Stat aid types, not the API's `MEASURE` codes. `Flags` is empty
  throughout the current release and reads as `float64` NaN, the same unstable-dtype caveat
  DAC1's bulk file carries.
- **Renames the DAC2 filter builder and schema-translation function to be DAC2-generic**, since
  both now serve DAC2a and DAC2b: `QueryBuilder.build_dac2a_filter` is now `build_dac2_filter`,
  and `convert_dac2a_to_dotstat_codes` is now `convert_dac2_to_dotstat_codes`. Both old names still
  work. They emit a `DeprecationWarning` and delegate to the new name, so existing calls to
  `build_dac2a_filter`, `convert_dac2a_to_dotstat_codes`, or `get_available_filters("dac2a")` are
  unaffected.
- **Renames the shared DAC2 schema mapping file** from `dac2a_dotstat.json` to `dac2_dotstat.json`.
  `read_schema_translation("dac2a")` and `read_schema_translation("dac2b")` both resolve to it
  through the existing alias mechanism (the same one `cpa` uses to reuse the CRS schema), so
  callers are unaffected.
- **Corrects the docs for `bulk_download_dac2a()`'s bulk file columns.** They were previously
  documented as PascalCase (`DonorCode`, `RecipientCode`), but the file returns the same 14
  paired code/label columns as `bulk_download_dac1()` and `bulk_download_dac2b()`:
  `RECIPIENT`/`Recipient`, `DONOR`/`Donor`, `PART`/`Part`, `AIDTYPE`/`Aid type`,
  `DATATYPE`/`Amount type`, `TIME`/`Year`, plus `Value` and `Flags`.

## 1.10.0 (2026-08-13)

- **Raises the Python floor to 3.12.** `requires-python` moves from `>=3.11` to `>=3.12`, the
  `Programming Language :: Python :: 3.11` classifier is dropped, and `ruff`'s `target-version` and
  `ty`'s `python-version` move to `3.12` to match. CI drops the `3.11` leg from the test matrix and
  picks up `3.14`, keeping three versions under test.
- **Moves `pandas-stubs` out of runtime dependencies.** It is a type-stub package that ships no
  runtime code — only `ty` reads it — so pinning it in `[project].dependencies` forced every
  consumer to install stubs they would never import. It now lives in the `dev` group, and its cap
  relaxes from `~=2.3.3` to `>=2.3.3`.
- **Adopts the bblocks family contract.** `.copier-answers.yml` now records `preset: family`,
  bringing `oda_reader` under `bblocks-projects doctor`'s contract checks.
- **Breaking: cache directory resolution now delegates to `readerkit.resolve_cache_dir`,
  changing the on-disk cache path and orphaning every existing cache.** `ODA_READER_CACHE_DIR`
  still redirects the root exactly as before, and `set_cache_dir()` / `reset_cache_dir()` /
  `get_cache_dir()` keep their names and signatures — but the resolved path now nests under a
  schema and app segment readerkit owns. With no override, the default moves from
  `~/Library/Caches/oda-reader/1.10.0` to `~/Library/Caches/readerkit/v1/oda-reader/1.10.0`
  (paths shown for macOS; XDG paths shift the same way on Linux). A `set_cache_dir(path)`
  override or an `ODA_READER_CACHE_DIR` value is now treated as a _root_ rather than the exact
  final directory: readerkit appends `v1/oda-reader/1.10.0` beneath it, whereas previously those
  two branches returned the configured path verbatim, with no version segment — only the
  platform-default branch was version-segmented before this release. For a default-path user
  this costs one re-download, on upgrade. For anyone pinning `ODA_READER_CACHE_DIR` or calling
  `set_cache_dir()` — a CI job or a shared workstation, say — it is not one-time: every branch
  now appends the full `oda_reader` version, so the resolved path changes on **every** release,
  including patch releases, and each one re-downloads and re-caches everything from scratch at
  the new path. The previous version's directory is left behind with nothing to reclaim it —
  the cache-eviction and legacy-cleanup helpers only ever see the current version's directory,
  never its siblings. `BBLOCKS_CACHE_DIR` is added as a new family-wide fallback, below the env
  var and above the platform default. Adds `readerkit` as a runtime dependency; the HTTP session
  layer (`requests`/`requests-cache`) is unchanged in this release.

## 1.9.0 (2026-08-05)

- **Adds `bulk_download_dac1()`.** DAC1 was the one dataset with an API reader and no bulk path.
  OECD publishes the full table as a single zipped CSV at
  `webfs-dcd.oecd.org/files/dotStat/DSD_DAC1/Table1_Data.zip`, 18.7 MB compressed and 205 MB
  expanded, covering 1,042,278 rows across 14 columns. The signature matches
  `bulk_download_dac2a()`: `save_to_path`, `as_iterator`, and `use_raw_cache`. OECD publishes no
  per-year DAC1 files, so the full table is the only bulk option and there is no
  `download_dac1_file()`.
- **DAC1 columns arrive in .Stat form, pairing a code with a label for each dimension**:
  `DONOR`/`Donor`, `PART`/`Part`, `AIDTYPE`/`Aid type`, `FLOWS`/`Fund flows`,
  `AMOUNTTYPE`/`Amount type`, `TIME`/`Year`, plus `Value` and `Flags`. Three label columns
  contain spaces, so reach them with `df["Aid type"]`. This differs from `download_dac1()`, which
  returns preprocessed lowercase names such as `donor_code`. `Flags` is empty throughout the
  current release and reads as `float64` NaN, so treat its dtype as unstable across OECD
  republishes.
- **A DAC1 cache hit costs one HEAD request.** The DAC1 annotation label carries no `-vYYYYMMDD`
  stamp, so revalidation uses the server's `ETag`, the same path DAC2A has taken since 1.8.0.
  Offline, invalidation falls back to the 30-day TTL, and the cached file works.
- **`as_iterator=True` on DAC1 bounds what the caller accumulates and leaves peak memory
  unchanged.** The file is a zipped CSV, which the package reads whole and then slices, matching
  `download_crs_file()`. Row-group streaming applies to the parquet bulk files.
- `save_to_path` writes `table1_data.parquet`, lowercased with spaces replaced by underscores.

## 1.8.0 (2026-08-05)

- **Fixes bulk downloads, which were completely broken.** OECD moved bulk files off
  `stats.oecd.org/wbos/fileview2.aspx?IDFile=<GUID>` to stable addresses at
  `webfs-dcd.oecd.org/files/dotStat/...`, and changed the SDMX dataflow annotations that carry
  those links from `LABEL|<GUID>` to `LABEL|<full URL>` with a `-vYYYYMMDD` label suffix that
  changes on every republish. Every bulk function — `bulk_download_crs`, `download_crs_file`,
  `bulk_download_dac2a`, `bulk_download_multisystem` — built a malformed URL from the old GUID
  scheme and failed. URLs are now resolved directly from the annotation text; there is no ID to
  reconstruct a URL from anymore.
- **Catches, before release, a data-correctness bug the URL migration above would otherwise have
  introduced.** The new OECD URLs are permanently stable (`CRS.parquet` is always the same
  address, unlike the old GUID URLs, which rotated on every republish), so an OECD republish
  became indistinguishable from a cache hit: the package would silently go on serving a
  previously-cached file for up to the 30-day raw-cache TTL, with no signal that the data was now
  a month stale. Fixed by tracking a publication-version token alongside each resolved URL: most
  bulk files carry a `-vYYYYMMDD` stamp in their annotation label, and a change to that stamp now
  forces a refetch. DAC2A's label carries no stamp, so a lightweight HEAD request against the
  resolved URL substitutes its `ETag`/`Last-Modified` for the same purpose — the one user-visible
  consequence being that a DAC2A cache hit is no longer strictly zero-network. If neither check
  can be made (offline, server error, headers absent), invalidation degrades to the existing
  time-based TTL, so cached files still work offline.
- **`CRS.parquet` and `CRS-reduced.parquet` are no longer ZIP archives.** OECD now serves the full
  CRS bulk files (1.18 GB and 296 MB) as bare parquet. The package detects the payload type by
  magic bytes and handles both a zip and a bare parquet response; every other bulk file
  (year-specific CRS, DAC2A, Multisystem) is still a zip.
- **`bulk_download_crs()` now returns snake_case columns (`year`, `donor_code`), while
  `download_crs_file()`, DAC2A and Multisystem still return PascalCase (`Year`, `DonorCode`).**
  This follows directly from the column names OECD ships inside the bare parquet files and is a
  user-visible break for any code that reads columns off `bulk_download_crs()` output — check
  `df.columns` before and after upgrading. See
  [Bulk Downloads](https://github.com/ONEcampaign/oda_reader/blob/main/docs/docs/bulk-downloads.md#bulk_download_crs-uses-different-column-casing-than-everything-else)
  for the full comparison.
- Bulk downloads now send browser-like headers (`User-Agent`, `Accept`, `Sec-Fetch-*`, `sec-ch-ua`).
  The new file host sits behind a Cloudflare challenge that returns 403 to default Python client
  headers.
- Adds recovery for a resolved bulk-download URL going dead between resolution and fetch — a
  retired dataflow version, or OECD renaming or removing a file. This is narrower than it sounds:
  routine republishing under a file's now-stable URL never reaches this path at all, since the
  version-token check above already forces a refetch before any request is made. On a 404 or 403
  with the originating dataflow available, the package re-resolves the URL — and its version
  token — with the 7-day annotation cache bypassed, and retries the download once.
- Fixes `download_crs_file(year, as_iterator=True)` and the other delimited (csv/txt) bulk files,
  which previously raised `ValueError: Streaming not supported for csv/txt files`. This yields row
  slices of a whole-file parse rather than a true streaming parse, deliberately: pandas'
  per-chunk dtype inference silently corrupted values on real 2024 CRS data (a leading zero
  dropped from `Interest1` when one chunk of that column looked purely numeric in isolation).
  `as_iterator` bounds what the caller accumulates, not the cost of the read.
- Fixes a zip-slip vulnerability in bulk-file extraction: zip member names were joined to the
  destination directory unsanitized, allowing a malicious archive to write outside it, and an
  absolute member name discarded the destination entirely rather than escaping it. Pre-existing,
  not introduced by the URL migration above.
- Zip extraction into `save_to_path` now preserves each member's directory structure instead of
  flattening every member to its basename, and two members that resolve to the same destination
  path — including two names that differ only by case, on a case-insensitive filesystem such as
  macOS APFS or Windows NTFS — now raise `ValueError` instead of one silently overwriting the
  other.
- Fixes a redirect-based host-allowlist bypass: bulk downloads and the general HTTP helpers used
  to follow redirects transparently, so a 3xx from an allowlisted host could carry the request
  off-host without the allowlist ever seeing the new target. Redirects are no longer followed
  blindly — each hop is re-validated (the `.oecd.org` allowlist for bulk downloads, same-origin
  for the general HTTP helpers), capped at 5 hops.
- Fixes corrupt cached bulk payloads surfacing during lazy iteration: corruption discovered mid-
  iteration (a bad zip member CRC, a malformed row group) now evicts the cached file, so the next
  call re-fetches instead of failing identically forever.
- Files written via `save_to_path` — both zip members and the bare-parquet CRS files — are now
  written atomically (a temp sibling file, then an atomic replace), so an interrupted or failed
  write no longer leaves a truncated file where a good one previously existed.
- Adds request timeouts to bulk downloads (10s connect / 60s read per attempt) where there
  previously were none, and bounded retry (3 retries, 1s/2s/4s backoff) on a 403, a truncated
  transfer, or a transient transport error (timeout, connection error, chunked-encoding error).
  Truncation is detected by comparing bytes actually written against the response's
  `Content-Length` header — the dominant real-world corruption mode for a large streamed download
  — and the check is skipped (not treated as a failure) when that header is absent or the response
  is content-encoded, where the comparison can't be made honestly; this catches accidental
  truncation, not tampering. A 404 and other permanent errors fail fast instead of burning the
  retry budget.
- **Deprecates `get_bulk_file_id`, which now always raises `RuntimeError`** pointing at
  `get_bulk_file_url` — the annotations it parsed no longer exist. The per-source equivalents
  (`get_full_crs_parquet_id`, `get_reduced_crs_parquet_id`, `get_year_crs_zip_id`,
  `get_full_dac2a_parquet_id`, `get_full_multisystem_id`) instead emit a `DeprecationWarning` and
  return a full URL rather than raising, since existing two-step caller code (an ID obtained here,
  then passed to `bulk_download_parquet`) can keep working unchanged. `bulk_download_parquet` now
  takes `url` in the position the deprecated `file_id` used to occupy; `file_id` still works but
  warns. `BULK_DOWNLOAD_URL` is retired — there's no longer a fixed prefix to combine with a file
  ID. The bulk file cache is invalidated by this release (cache keys are now a hash of the new
  URLs, not the old ones).

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
