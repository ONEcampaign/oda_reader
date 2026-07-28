# Codelists Reference

The full contract for `oda_reader.codelists`. For the common paths and what each frame's key means,
start at [Codelists](codelists.md).

```python
from oda_reader.codelists import fetch_codelists, parse_codelists, reconcile
```

The five exception classes are importable from `oda_reader` directly.

## `CodelistSnapshot`

The return type of every fetch and parse function.

**Attributes**

| Attribute          | Type                                    | Description                                                                                                                                      |
| ------------------ | --------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| `contract`         | `Literal["area", "category", "agency"]` | Which fetch/parse pair built this snapshot. Determines `frame`'s columns and row grain, and is what `reconcile` checks.                          |
| `frame`            | `pd.DataFrame`                          | The codelist data. See [the area frame](#the-area-frame), [the category frame](#the-category-frame) and [the agency frame](#the-agency-frame).   |
| `raw`              | `Mapping[str, bytes]`                   | `codelist_id` to the exact response bytes for that codelist, unmodified. Read-only.                                                              |
| `fetched_at`       | `datetime`                              | When the bytes were obtained. Timezone-aware.                                                                                                    |
| `source_url`       | `str`                                   | Where the bytes came from.                                                                                                                       |
| `codelist_ids`     | `tuple[str, ...]`                       | The codelists this snapshot covers, in request order.                                                                                            |
| `content_hash`     | `str`                                   | A version-prefixed digest over the frame's contract columns. Equal hashes mean equal frame content. Hashes don't compare across prefix versions. |
| `unknown_statuses` | `tuple[str, ...]`                       | Distinct `status` values seen outside `STATUS_DOMAIN`, in first-seen order. Empty against the current source.                                    |

**To compare two snapshots, compare `content_hash`.** `==` on snapshots compares object identity, so
two snapshots built from the same bytes are never equal.

```python
if snapshot.content_hash == stored_hash:
    print("OECD published nothing new")
```

**Attributes can't be reassigned, but `frame` can be edited in place.** Copy it before mutating if
anything downstream expects the fetched values.

## Area codelists

### `fetch_codelists`

```python
fetch_codelists(*, codelist_ids=("5", "13"), timeout=30, retries=2) -> CodelistSnapshot
```

Fetches area codelists from the live OECD page and parses them.

**Parameters**

- **`codelist_ids`** (`Sequence[str]`, default: `("5", "13")`) — which codelists to fetch, in request
  order. Each must be `"5"` or `"13"`. Duplicates are rejected. Pass a sequence, never a bare string.
- **`timeout`** (`int`, default: `30`) — per-request timeout in seconds, applied to every request of
  every codelist's handshake.
- **`retries`** (`int`, default: `2`) — extra attempts after the first, per codelist. Total attempts
  per codelist is `retries + 1`.

**Returns** — `CodelistSnapshot` covering every id in `codelist_ids`.

**Raises**

- **`CodelistValidationError`** — `codelist_ids` is a bare `str`, empty, holds a non-`str` element,
  names an unsupported codelist, or repeats one. All raised before any request goes out. Id `"16"`
  names the agency contract instead of reporting a bare "unsupported". Also raised when a codelist's
  payload parses to zero rows.
- **`CodelistFetchError`** — transport failure, or 5xx/429/408/425 that survived every attempt.
- **`CodelistSourceError`** — a 4xx, a refused redirect, or a 200 that isn't the CodesList.aspx page.
- **`CodelistShapeError`** — a required handshake token was missing, or the payload's shape isn't
  what the parser expects.

**Example**

```python
from oda_reader import CodelistValidationError
from oda_reader.codelists import fetch_codelists

try:
    fetch_codelists(codelist_ids=("10",))
except CodelistValidationError as exc:
    print(exc)
# Codelist validation error: unsupported codelist_id '10'; supported: ('5', '13') [codelist_id=10]
```

Status is not a parameter. The handshake requests every status, so a call returns whatever states the
source carries. Today that is `active` and `withdrawn`; no `future` or `heading` row has been
observed.

### `parse_codelists`

```python
parse_codelists(*, raw, fetched_at, source_url=DEFAULT_URL) -> CodelistSnapshot
```

Builds the same `CodelistSnapshot` from bytes already in hand. No network access.

**Parameters**

- **`raw`** (`Mapping[str, bytes]`) — `codelist_id` to that codelist's response bytes, in the order
  `codelist_ids` should report.
- **`fetched_at`** (`datetime`) — when `raw` was obtained. Must be timezone-aware. Use
  `datetime.datetime.now(datetime.UTC)`.
- **`source_url`** (`str`, default: the OECD CodesList.aspx URL) — recorded on the snapshot. Does not
  affect parsing.

**Returns** — `CodelistSnapshot`. Every guarantee documented on the type holds on bytes obtained any
other way.

**Raises**

- **`CodelistShapeError`** — a payload isn't JSON, its envelope is malformed, or a row is missing a
  required field, carries a malformed `activation_date`, or conflicts with another row sharing its
  key.
- **`CodelistValidationError`** — `raw` is empty, a codelist in `raw` produced zero rows, or
  `fetched_at` is naive.

### The area frame

Ten columns, one row per `(codelist_id, code, activation_date)`. Every column is `string[pyarrow]`
except `activation_date`, which is `date32[pyarrow]`. Missing values are `pd.NA`, never an empty
string.

| Column            | Source field              | Null when                                                                                |
| ----------------- | ------------------------- | ---------------------------------------------------------------------------------------- |
| `codelist_id`     | request parameter         | never                                                                                    |
| `code`            | `code`                    | never                                                                                    |
| `label`           | `name.narrative`, English | never                                                                                    |
| `status`          | `status`, lowercased      | never                                                                                    |
| `type`            | `type`                    | rare, one provider row today                                                             |
| `dotstat_code`    | `dotstatcode`             | 32 rows: 17 multilaterals and 14 countries in codelist 5, plus code `999` in codelist 13 |
| `iso3`            | `iso-alpha-3-code`        | 172 of 577 rows, mostly multilaterals, private donors and regional aggregates            |
| `crs`             | `crs` (`"0"` / `"1"`)     | never observed                                                                           |
| `tossd`           | `tossd` (`"0"` / `"1"`)   | never observed                                                                           |
| `activation_date` | `activation-date`         | all 213 codelist-5 rows, plus code `999` (Global) in codelist 13                         |

**`status`** is always lowercase, so codelist 5's `"Active"` and codelist 13's `"active"` both arrive
as `"active"`. The documented domain is `STATUS_DOMAIN`: `active`, `withdrawn`, `future`, `heading`.
A value outside it reaches the frame unchanged and is listed in `snapshot.unknown_statuses`.

**`label`** is guaranteed to be English. Nothing else about how it's sourced is.

The three-column key is unique across both area codelists. A code carries [one row per validity
period](codelists.md#area-one-row-per-validity-period).

## Category codelists

The 23 flat codelists other than the area and agency ones. Handshake, retry loop, exception taxonomy,
`content_hash` and `raw` replayability are shared with the area contract.

### `fetch_code_categories`

```python
fetch_code_categories(*, codelist_ids, timeout=30, retries=2) -> CodelistSnapshot
```

Fetches category codelists from the live OECD page and parses them.

**Parameters**

- **`codelist_ids`** (`Sequence[str]`) — which codelists to fetch, in request order. Required, no
  default, since requesting all 23 is a ~900 KB fetch. Each must be one of the 23 ids in
  `SUPPORTED_CATEGORY_IDS`. Duplicates are rejected. Pass a sequence, never a bare string.
- **`timeout`** (`int`, default: `30`) — as `fetch_codelists`.
- **`retries`** (`int`, default: `2`) — as `fetch_codelists`.

**Returns** — `CodelistSnapshot` with the eleven-column category frame.

**Raises** — the same four classes as `fetch_codelists`. Ids `"5"`, `"13"` and `"16"` name the
contract that owns them instead of reporting a bare "unsupported":

```text
Codelist validation error: codelist_id '16' (Provider agency) is an agency codelist; an agency code is only meaningful within its donor, so agency rows are keyed on (donor-code, code) rather than the flat code a category codelist uses -- use fetch_provider_agencies / parse_provider_agencies instead of the category contract [codelist_id=16]
```

**Example**

```python
from oda_reader.codelists import fetch_code_categories

snapshot = fetch_code_categories(codelist_ids=("10", "3"))
snapshot.frame.head()
```

### `parse_code_categories`

```python
parse_code_categories(*, raw, fetched_at, source_url=DEFAULT_URL) -> CodelistSnapshot
```

Builds a category `CodelistSnapshot` from bytes already in hand. No network access.

Parameters, return type and exceptions match `parse_codelists`, over the category frame. `raw` keys
are not validated against the supported set.

```python
import datetime
from pathlib import Path

from oda_reader.codelists import parse_code_categories

raw = {"10": Path("codelist_10.json").read_bytes()}
snapshot = parse_code_categories(
    raw=raw,
    fetched_at=datetime.datetime(2026, 7, 20, tzinfo=datetime.UTC),
)
```

### The category frame

Eleven columns, one row per `(codelist_id, code, activation_date, crs, tossd)`. `activation_date` is
`date32[pyarrow]`, the rest are `string[pyarrow]`. Missing values are `pd.NA`.

| Column            | Source field                     | Null when                                                                                       |
| ----------------- | -------------------------------- | ----------------------------------------------------------------------------------------------- |
| `codelist_id`     | request parameter                | never                                                                                           |
| `code`            | `code`                           | never                                                                                           |
| `label`           | `name.narrative`, English        | never                                                                                           |
| `status`          | `status`, lowercased             | never                                                                                           |
| `crs`             | `crs` (`"0"` / `"1"`)            | never observed                                                                                  |
| `tossd`           | `tossd` (`"0"` / `"1"`)          | never observed                                                                                  |
| `activation_date` | `activation-date`                | absent on every row of 7 of the 23 codelists, present on every row of 9, partial in the other 7 |
| `description`     | `description.narrative`, English | absent on every row of 13 of the 23                                                             |
| `category`        | `category`                       | absent outside Purpose code, Channel of delivery, Type of finance and Co-operation modality     |
| `parent_code`     | `parent-code`                    | absent outside Purpose code. See [`parent_code`](#parent_code)                                  |
| `dac_reference`   | `dac:reference`                  | absent on every row of 13 of the 23                                                             |

`status` and `label` follow the area frame's rules.

Sparse columns are expected. In Currency (`"4"`), the `description`, `category`, `parent_code` and
`dac_reference` columns are `pd.NA` on every row.

The five-column key is unique across all 23 supported codelists. A code repeats within a codelist for
two separate reasons: four codelists carry [one row per reporting
standard](codelists.md#category-one-row-per-standard-and-period), and `activation_date` splits a code
into validity periods the same way it does in the area frame.

### `parent_code`

Present only in Purpose code (`"10"`), where it encodes the CRS sector hierarchy. Code `111`'s
`parent_code` is `110`, the sector row it sits under. Every non-null parent reference resolves to a
code in the same codelist.

The source sends an empty string rather than omitting the field, which normalises to `pd.NA`. Of
Purpose code's 392 rows, 124 carry the `parent-code` key and 81 carry a real reference, so
`frame["parent_code"].notna()` returns 81.

### `dac_reference`

Free-text document citations from the source's `dac:reference` field, present in 10 of the 23
codelists. Real values:

```text
DCD/DAC/STAT(2015)23
DCD/DAC/STAT(2009)1;DCD/DAC/STAT(2018)42;DCD/DAC/STAT(2021)12/ADD1/REV1
Renamed from PDGG: DCD/DAC/STAT(2021)15/REV1
```

One citation, several joined by `;`, or a citation with changelog prose attached. The format is free
text with no guaranteed structure. Read it, don't parse it, split it, or join on it.

## Provider agencies

Codelist 16, the agencies that sit under each provider.

### `fetch_provider_agencies`

```python
fetch_provider_agencies(*, timeout=30, retries=2) -> CodelistSnapshot
```

Fetches codelist 16 from the live OECD page and parses it.

**Parameters**

- **`timeout`** (`int`, default: `30`) — per-request timeout in seconds, applied to every request of
  the handshake.
- **`retries`** (`int`, default: `2`) — extra attempts after the first. Total attempts is
  `retries + 1`.

There is no `codelist_ids` parameter. Codelist 16 is the only agency codelist.

**Returns** — `CodelistSnapshot` with the twelve-column agency frame.

**Raises** — the same four classes as `fetch_codelists`.

**Example**

```python
from oda_reader.codelists import fetch_provider_agencies

agencies = fetch_provider_agencies().frame
```

### `parse_provider_agencies`

```python
parse_provider_agencies(*, raw, fetched_at, source_url=DEFAULT_URL) -> CodelistSnapshot
```

Builds an agency `CodelistSnapshot` from bytes already in hand. No network access.

Parameters, return type and exceptions match `parse_codelists`, over the agency frame. `raw` is a
single-entry mapping keyed on the one id in `SUPPORTED_AGENCY_IDS`, and its keys are not validated.

### The agency frame

Twelve columns, one row per `(codelist_id, donor_code, code, activation_date)`. `activation_date` is
`date32[pyarrow]`, the rest are `string[pyarrow]`. Missing values are `pd.NA`.

| Column             | Source field                | Null when         |
| ------------------ | --------------------------- | ----------------- |
| `codelist_id`      | request parameter           | never             |
| `donor_code`       | `donor-code`                | never             |
| `code`             | `code`                      | never             |
| `label`            | `name.narrative`, English   | never             |
| `status`           | `status`, lowercased        | never             |
| `acronym`          | `acronym`, English          | 549 of 1,374 rows |
| `agency_type_code` | `Agencytype.code`           | never             |
| `agency_type`      | `Agencytype.name`, English  | never             |
| `crs`              | `crs` (`"0"` / `"1"`)       | never observed    |
| `tossd`            | `tossd` (`"0"` / `"1"`)     | never observed    |
| `used_in_cpa`      | `UsedinCPA` (`"0"` / `"1"`) | 331 of 1,374 rows |
| `activation_date`  | `activation-date`           | every row today   |

`donor_code` leads the key ahead of `code`, since an agency code is only meaningful within its donor.
The frame holds 1,374 rows over 110 distinct agency codes across 198 donors, with [one row per
donor](codelists.md#agency-one-row-per-donor) for a shared code.

`crs` and `tossd` stay out of the key, unlike the category frame. Agency rows do not split by
reporting standard.

## `reconcile`

```python
reconcile(*, previous, current, observed_at=None) -> Reconciliation
```

Merges a previous codelist table with a fresh snapshot and reports what moved. Deterministic given
its inputs, and it never deletes a row.
[Reconciling against history](codelists.md#reconciling-against-history) shows the workflow and the
three behaviours that matter when alerting on the result.

**Parameters**

- **`previous`** (`pd.DataFrame | None`) — an existing table, or `None` on first run. May be a plain
  snapshot frame with no lineage columns, which are backfilled. Contract columns are coerced to
  contract dtypes on entry, so a table read back as `int64`/`object` works without manual casting.
- **`current`** (`CodelistSnapshot`) — a snapshot from `fetch_codelists` or `parse_codelists`.
- **`observed_at`** (`date | None`, default: `None`) — the lineage date. Defaults to
  `current.fetched_at` as a UTC date.

**Returns** — `Reconciliation`.

**Raises**

- **`CodelistValidationError`** — `current.contract` is not `"area"`, or its frame is not the area
  contract's ten columns; `previous` or `current` has duplicate keys; a `previous` column can't be
  coerced without loss; `presence` holds a value outside `PRESENCE_DOMAIN`; or `observed_at`
  predates a `last_seen` already in `previous`.

### `Reconciliation`

**Attributes**

- **`table`** (`pd.DataFrame`) — the union of `previous` and `current`, ready to load. Carries the
  ten contract columns, the four lineage columns, and any extra column `previous` had.
- **`added`** (`pd.DataFrame`) — rows whose key was absent from `previous`. Full `table` columns.
- **`retired`** (`pd.DataFrame`) — rows that moved to `presence="retired"` this run. Full `table`
  columns.
- **`changed`** (`pd.DataFrame`) — one row per changed value, with columns `codelist_id`, `code`,
  `activation_date`, `column`, `old_value`, `new_value`, `observed_at`.
- **`is_unchanged`** (`bool`) — `True` only when `added`, `retired` and `changed` are all empty.

The four lineage columns `table` adds on top of the frame's ten, named by `LINEAGE_COLUMNS`:

| Column          | Dtype             | Description                                                              |
| --------------- | ----------------- | ------------------------------------------------------------------------ |
| `first_seen`    | `date32[pyarrow]` | When this code was first observed. `pd.NA` if `previous` had no lineage. |
| `last_seen`     | `date32[pyarrow]` | When this code last appeared in a live payload.                          |
| `source_status` | `string[pyarrow]` | OECD's own status at `last_seen`.                                        |
| `presence`      | `string[pyarrow]` | `current` or `retired`. Never null.                                      |

## Module constants

Exported from `oda_reader.codelists`.

| Constant                 | Value                                                          |
| ------------------------ | -------------------------------------------------------------- |
| `SUPPORTED_CODELIST_IDS` | `("5", "13")`                                                  |
| `SUPPORTED_CATEGORY_IDS` | The 23 category ids, `"1"` through `"26"` less `5`, `13`, `16` |
| `SUPPORTED_AGENCY_IDS`   | `("16",)`                                                      |
| `STATUS_DOMAIN`          | `("active", "withdrawn", "future", "heading")`                 |
| `PRESENCE_DOMAIN`        | `("current", "retired")`                                       |
| `LINEAGE_COLUMNS`        | `("first_seen", "last_seen", "source_status", "presence")`     |

```python
from oda_reader.codelists import STATUS_DOMAIN, fetch_codelists

snapshot = fetch_codelists()
assert snapshot.frame["status"].isin(STATUS_DOMAIN).all()
```

## Exceptions

Five classes, all importable from `oda_reader`, all inheriting from `CodelistError` and from
`Exception` only. None subclass `OSError` or `ConnectionError`, so `except OSError` won't catch them.
Every contract raises the same five.

| Failure                                                                          | Exception                 | `is_retryable` |
| -------------------------------------------------------------------------------- | ------------------------- | -------------- |
| Network timeout or reset, HTTP 5xx/429/408/425, exhausted after internal retries | `CodelistFetchError`      | `True`         |
| HTTP 404/403/other 4xx, or a cross-origin redirect                               | `CodelistSourceError`     | `False`        |
| A 200 response that isn't the OECD codelist page                                 | `CodelistSourceError`     | `False`        |
| A required token missing from the handshake page                                 | `CodelistShapeError`      | `False`        |
| The JSON envelope isn't the expected shape                                       | `CodelistShapeError`      | `False`        |
| A row missing a required field, or conflicting with another row on its key       | `CodelistShapeError`      | `False`        |
| `raw` is empty, or a codelist returned zero rows                                 | `CodelistValidationError` | `False`        |
| `codelist_ids` is a bare `str`, empty, non-`str`, unsupported, or duplicated     | `CodelistValidationError` | `False`        |
| `fetched_at` is naive                                                            | `CodelistValidationError` | `False`        |

Every instance carries `codelist_id`, or `None` before it's known. `CodelistFetchError` adds `url`,
`stage`, `attempts` and `status_code`. `CodelistSourceError` adds `url`, `stage`, `status_code`,
`final_url`, `location` and a bounded, token-redacted `body`. `CodelistShapeError` adds `stage`,
`detail`, `url` and the same bounded `body`. `CodelistValidationError` adds `detail`.

**Redirects** — the OECD app answers a codelist selection with a same-origin redirect, which is
followed automatically and is part of the normal handshake. A redirect to a different host is refused
and raises `CodelistSourceError`.

## Next

- **[Codelists](codelists.md)** - The guide, including what one row means in each frame and the
  reconcile workflow
- **[Codelist ID Reference](codelist-ids.md)** - All 26 OECD ids, their names, and which contract
  covers them
