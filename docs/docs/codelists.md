# Codelists

`oda_reader.codelists` fetches OECD DAC codelists — the reference tables behind the data, not
the data itself — straight from the OECD source, as a pandas DataFrame. It ships two contracts
over that source:

- **The area contract** (`fetch_codelists` / `parse_codelists`), covering the two codelists most
  pipelines join against: providers (`"5"`) and recipients (`"13"`).
- **The category contract** (`fetch_code_categories` / `parse_code_categories`), covering the
  other 23 flat OECD DAC codelists — purpose codes, channels of delivery, markers, and the rest.
  See [The category contract](#the-category-contract) below.

It exists for the same reason the rest of `oda_reader` exists: OECD publishes this reference data
behind a fragile, undocumented interface, and pipelines that depend on it need something more
durable than a scraper copy-pasted between projects.

`oda_reader.codelists` is **not** re-exported from `oda_reader`. `import oda_reader` stays
network-free by design — nothing it imports opens a socket — so the submodule is imported
explicitly:

```python
from oda_reader.codelists import fetch_codelists
```

The five exception classes it can raise, described below, are the one exception to this: they
are available directly from `oda_reader`, so you can write `except oda_reader.CodelistFetchError`
without importing the submodule just to catch a failure. They're shared by both contracts.

It's named `fetch_codelists`, not `download_*` like the rest of the package's readers, on purpose:
`download_*` functions hand back a bare DataFrame, while `fetch_codelists` returns a
`CodelistSnapshot` — the frame plus the raw bytes, fetch time and source URL behind it. The
different verb signals the different return contract before you've read the signature. The
category contract's `fetch_code_categories` follows the same naming for the same reason.

## Fetching a snapshot

```python
from oda_reader.codelists import fetch_codelists

snapshot = fetch_codelists()
snapshot.frame.head()
```

`fetch_codelists` fetches both supported codelists by default and returns a `CodelistSnapshot`:

```python
def fetch_codelists(
    *,
    codelist_ids: Sequence[str] = ("5", "13"),
    timeout: int = 30,
    retries: int = 2,
) -> CodelistSnapshot: ...
```

`CodelistSnapshot` holds everything one fetch produced:

| Attribute          | Type                  | Meaning                                                                                                                                                          |
| ------------------ | --------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `frame`            | `pd.DataFrame`        | The codelist data — see [The frame](#the-frame) below.                                                                                                           |
| `raw`              | `Mapping[str, bytes]` | `codelist_id -> the exact response bytes`, unmodified, as OECD sent them.                                                                                        |
| `fetched_at`       | `datetime`            | When the underlying bytes were obtained. Timezone-aware.                                                                                                         |
| `source_url`       | `str`                 | Where the bytes came from.                                                                                                                                       |
| `codelist_ids`     | `tuple[str, ...]`     | The codelists this snapshot covers, in request order.                                                                                                            |
| `content_hash`     | `str`                 | `"v1:"` followed by a sha256 hex digest of the frame's contract columns. Two snapshots with equal `content_hash` have equal frame content.                       |
| `unknown_statuses` | `tuple[str, ...]`     | Distinct `status` values seen outside the documented domain (`active`, `withdrawn`, `future`, `heading`), in first-seen order. Empty against the current source. |

`CodelistSnapshot` is a frozen dataclass with `eq=False`: two snapshots holding identical data
still compare unequal under `==`, because a generated equality check over a DataFrame doesn't
work. Compare snapshots by `content_hash`, not `==`.

### `parse_codelists`: the escape hatch

```python
def parse_codelists(
    *,
    raw: Mapping[str, bytes],
    fetched_at: datetime,
    source_url: str = DEFAULT_URL,
) -> CodelistSnapshot: ...
```

`fetch_codelists` is `parse_codelists` composed with the live handshake against OECD. Nothing
about `parse_codelists` is a convenience wrapper you can skip reading about: it is what keeps a
pipeline running when the OECD page breaks. Every guarantee documented on `CodelistSnapshot` holds
on bytes obtained any other way — yesterday's fetch, a colleague's manual download, a payload your
own pipeline already has stored — with no network access and no release from us on the critical
path.

```python
import json
from pathlib import Path

raw = {
    "5": Path("codelist_5.json").read_bytes(),
    "13": Path("codelist_13.json").read_bytes(),
}
snapshot = parse_codelists(raw=raw, fetched_at=your_stored_fetch_time)
```

`fetched_at` must be timezone-aware — `datetime.datetime.now()` without `datetime.UTC` is naive
and is rejected with `CodelistValidationError`, rather than silently assumed to be UTC. `reconcile`
later converts it with `.astimezone(UTC)`, and a naive value there would be reinterpreted in the
machine's local timezone, landing `last_seen` on the wrong date. Use
`datetime.datetime.now(datetime.UTC)`.

If you keep dated raw snapshots of your own — and [Being a good citizen](#being-a-good-citizen)
below recommends that you do — `parse_codelists` is what makes them useful for something other
than an audit trail: a pipeline built around it degrades to "the last payload we have, plus an
alert" instead of being blocked on a release from us.

## Supported codelists

Only two codelist ids are supported: `"5"` (providers) and `"13"` (recipients). Passing anything
else raises `CodelistValidationError`, before any request is made:

```python
from oda_reader.codelists import fetch_codelists
from oda_reader import CodelistValidationError

try:
    fetch_codelists(codelist_ids=("10",))
except CodelistValidationError as exc:
    print(exc)
# Codelist validation error: unsupported codelist_id '10'; supported: ('5', '13')
```

OECD publishes 27 dropdown options in total: 26 codelists plus "All codes list" (`id=0`), which
returns all of them in one response. Of the 24 non-area codelists, 23 are covered by
[the category contract](#the-category-contract), documented later on this page; the remaining
one, Provider agency, is out of scope entirely — also explained there.

## The frame

`snapshot.frame` has ten columns, all `string[pyarrow]` except `activation_date`. Missing values
are `pd.NA`, never an empty string.

| Column            | Source field              | Dtype             | Null when                          |
| ----------------- | ------------------------- | ----------------- | ---------------------------------- |
| `codelist_id`     | request parameter         | `string[pyarrow]` | never                              |
| `code`            | `code`                    | `string[pyarrow]` | never                              |
| `label`           | `name.narrative`, English | `string[pyarrow]` | never                              |
| `status`          | `status`, lowercased      | `string[pyarrow]` | never                              |
| `type`            | `type`                    | `string[pyarrow]` | rare, one provider row today       |
| `dotstat_code`    | `dotstatcode`             | `string[pyarrow]` | multilaterals, some withdrawn rows |
| `iso3`            | `iso-alpha-3-code`        | `string[pyarrow]` | every non-country                  |
| `crs`             | `crs` (`"0"`/`"1"`)       | `string[pyarrow]` | never observed                     |
| `tossd`           | `tossd` (`"0"`/`"1"`)     | `string[pyarrow]` | never observed                     |
| `activation_date` | `activation-date`         | `date32[pyarrow]` | every row in codelist 5            |

`status` is normalised to lowercase at the boundary — codelist 5 returns `"Active"`, codelist 13
returns `"active"`, and both become `"active"` in the frame — and constrained to the domain
`active` / `withdrawn` / `future` / `heading`. A value outside that domain is not dropped: it
passes through the frame unchanged and is listed in `snapshot.unknown_statuses`.

`label` is a promise about *language*, not about how it's located. We return the narrative with no
`xml:lang` attribute (OECD tags the French variant, not the English one), falling back to the
first entry if none matches. If OECD changes how it marks the English narrative, we fix the
selector in a patch release; only a change to *which language* `label` carries is a breaking
change.

### The row grain

**One row per `(codelist_id, code, activation_date)` — not one row per code.** This is the single
most surprising thing about the frame, and assuming otherwise produces a join that is silently
wrong rather than one that fails loudly.

OECD publishes codelist 13 as validity periods: fetching it returns 364 rows over 207 distinct
codes, because 116 codes carry two to six rows each — one per period of their history. Code `130`
(Algeria) is the clearest example, with three rows:

```
code   status      activation_date   iso3   label
130    withdrawn   1996-01-01        DZA    Algeria
130    withdrawn   2011-01-01        DZA    Algeria
130    active      2022-01-01        DZA    Algeria
```

These are one entity's history, not three different codes, and all three stay in the frame.
Across the two supported codelists the full frame is 577 rows (213 from codelist 5, 364 from
codelist 13), and the three-column key is unique — no duplicate `(codelist_id, code, activation_date)` combinations.

Two consequences follow directly:

- **`status` is a property of a period, not of a code.** "Is code 130 active?" means selecting the
  row with the greatest `activation_date` for that code, not filtering `status == "active"` over
  the whole frame — filtering happens to give the same answer today, because no code has more than
  one `"active"` row, but it is answering a different question.
- **`pd.NA` is a legitimate part of the key.** Codelist 5 carries no `activation_date` at all, so
  every one of its 213 rows keys on a null third component and stays unique on `code` alone. Any
  `groupby` or merge you write on the key needs to be `pd.NA`-aware (`dropna=False` on a `groupby`,
  for instance) — the default pandas behaviour silently drops rows with a null key.

## Reconciling against history

`fetch_codelists` gives you this week's answer. Most pipelines also need last week's answer, and a
reliable way to tell the two apart — which recipient code just got added, which one OECD quietly
stopped listing, which label changed under you. `reconcile` is that comparison, and it exists
because a warehouse table has none of git's protections: no durable prior state, no diff a human
reviews before it lands, no way to undo a bad `CREATE OR REPLACE`.

> `oda_reader.codelists` never tells you a code stopped existing. It tells you a code stopped
> being listed. Give us your previous table and the table we give back is a superset of it.

```python
from oda_reader.codelists import fetch_codelists, reconcile

snap = fetch_codelists()
previous = con.sql("select * from codelists.dac_codelists").df() if table_exists else None
rec = reconcile(previous=previous, current=snap)

con.execute("create or replace table codelists.dac_codelists as select * from rec.table")
```

`reconcile` is a pure function — no I/O, no state, deterministic given its inputs. It never
deletes a row: a code OECD stops listing comes back in `rec.table` with `presence="retired"`, its
last known values intact, rather than vanishing the moment your `CREATE OR REPLACE` runs. The only
way to lose a code is to not pass `previous`.

`rec.table` carries the ten frame columns plus four lineage columns:

| Column          | Dtype             | Meaning                                                                 |
| --------------- | ----------------- | ----------------------------------------------------------------------- |
| `first_seen`    | `date32[pyarrow]` | Date we first observed this code. `pd.NA` if `previous` had no lineage. |
| `last_seen`     | `date32[pyarrow]` | Date this code last appeared in a live payload.                         |
| `source_status` | `string[pyarrow]` | OECD's own status at `last_seen`.                                       |
| `presence`      | `string[pyarrow]` | `current` or `retired`. Exactly those two values.                       |

Any column your own table already had beyond these — a load timestamp, a source tag — survives
into `rec.table` unchanged, with `pd.NA` for a code that's new this run.

`rec.added`, `rec.retired` and `rec.changed` are the report: full-row frames (`added`/`retired`)
or a `(codelist_id, code, activation_date, column, old_value, new_value, observed_at)` diff
(`changed`) that a human never has to derive by hand. `rec.is_unchanged` is `True` only when all
three are empty — advancing `last_seen` on an otherwise-identical row does not count as a change.

A few of the rules behind that guarantee are easy to get backwards, so they're worth stating
explicitly:

- **Retirement is scoped to the codelists you actually fetched.** A previous row whose
  `codelist_id` wasn't among this run's `codelist_ids` passes through untouched — same `presence`,
  same `last_seen`. If you fetch only codelist 5 during an incident and reconcile against your
  full table, codelist 13 is not mass-retired; absent-because-not-requested is not retirement.
- **A code that reappears after being retired is a change, not an addition.** It returns to
  `presence="current"`, keeps its original `first_seen`, and shows up in `rec.changed` with
  `column="presence"` — never in `rec.added`. An alert wired to `rec.added` should not fire just
  because a code came back.
- **`previous` is coerced to the frame's contract dtypes on entry**, because it will almost never
  arrive with them already — a DuckDB `.df()` hands back `int64`/`float64`/`object` columns, not
  `string[pyarrow]`. `reconcile` handles that on your behalf, including the case where a nullable
  integer column comes back as `float64` and would otherwise turn `code` `5` into the string
  `"5.0"`.

We do not build a slowly-changing-dimension table here, and don't intend to: bitemporal semantics
are hard to get right, harder to change once someone's queries depend on them, and are your
pattern to choose, not ours. `rec.changed` gives you exactly the input to an append-only audit
table if you want one.

**What `reconcile` deliberately doesn't do: decide whether a change matters.** `rec.added`,
`rec.retired` and `rec.changed` are handed to you as small frames so you write your own assertion,
with your own thresholds, in your own orchestrator — a blocking Dagster asset check, a dbt test, a
Slack alert past some size, whatever fits. We supply the evidence and refuse to supply the policy.

## The category contract

`fetch_code_categories` and `parse_code_categories` are the category-contract counterparts of
`fetch_codelists` and `parse_codelists`, over the 23 flat non-area OECD DAC codelists — purpose
codes, channels of delivery, markers, and everything else that isn't Provider or Recipient. Same
handshake, same retry loop, same exception taxonomy, same `content_hash` and `raw` replayability
— everything below the frame is shared, not reimplemented. What differs is the frame itself: a
different (larger) set of columns, and a different, wider row key.

```python
from oda_reader.codelists import fetch_code_categories

snapshot = fetch_code_categories(codelist_ids=("10", "3"))
snapshot.frame.head()
```

```python
def fetch_code_categories(
    *,
    codelist_ids: Sequence[str],
    timeout: int = 30,
    retries: int = 2,
) -> CodelistSnapshot: ...
```

Unlike `fetch_codelists`, **`codelist_ids` has no default.** `fetch_codelists`'s default,
`("5", "13")`, is the whole area-contract supported set — a bare call fetches everything it knows
about. There is no equivalent sensible default across 23 codelists, and defaulting to all of them
would make a ~1.7 MB fetch (the size of the "All codes list" response, which is what requesting
all 23 amounts to) the accidental behaviour of a bare call. You say which codelists you want.

`parse_code_categories` is the same escape hatch `parse_codelists` is, over the category frame:

```python
def parse_code_categories(
    *,
    raw: Mapping[str, bytes],
    fetched_at: datetime,
    source_url: str = DEFAULT_URL,
) -> CodelistSnapshot: ...
```

```python
import datetime
from pathlib import Path
from oda_reader.codelists import parse_code_categories

raw = {
    "10": Path("codelist_10.json").read_bytes(),
    "3": Path("codelist_3.json").read_bytes(),
}
snapshot = parse_code_categories(raw=raw, fetched_at=datetime.datetime.now(datetime.UTC))
```

Both return the same `CodelistSnapshot` type `fetch_codelists` returns, not a distinct type: one
return type means one place `content_hash`, `raw` replayability and `unknown_statuses` are
documented, instead of two copies of the same guarantees that could drift apart. You always know
which frame contract you're holding, because you know which function you called — the same way
`download_dac1` and `download_crs` both return a bare `pd.DataFrame` with entirely different
columns, without anyone finding that confusing.

### Supported codelists

23 ids: `"1"`, `"2"`, `"3"`, `"4"`, `"6"`, `"7"`, `"8"`, `"9"`, `"10"`, `"11"`, `"12"`, `"14"`,
`"15"`, `"17"`, `"18"`, `"19"`, `"20"`, `"21"`, `"22"`, `"23"`, `"24"`, `"25"`, `"26"`. All 23 ship
at once — the parser is identical for every one of them, so shipping some and deferring the rest
would only buy a drip of minor releases and a supported-ids set that reads as arbitrary.

Three ids outside that set get a specific error message rather than a bare "unsupported", because
they're the wrong ids a category-contract caller is most likely to reach for:

```python
from oda_reader.codelists import fetch_code_categories
from oda_reader import CodelistValidationError

for codelist_id in ("5", "13", "16"):
    try:
        fetch_code_categories(codelist_ids=(codelist_id,))
    except CodelistValidationError as exc:
        print(exc)
```

```text
Codelist validation error: codelist_id '5' (Provider) is an area codelist; use fetch_codelists / parse_codelists instead of the category contract [codelist_id=5]
Codelist validation error: codelist_id '13' (Recipient) is an area codelist; use fetch_codelists / parse_codelists instead of the category contract [codelist_id=13]
Codelist validation error: codelist_id '16' (Provider agency) is not a flat category codelist -- it describes agencies within providers, keyed on (code, activation_date, donor-code), a different entity per donor, and is out of scope for the category contract entirely [codelist_id=16]
```

- **`"5"` and `"13"`** (Provider, Recipient) belong to the area contract — use `fetch_codelists` /
  `parse_codelists` instead.
- **`"16"`** (Provider agency) is excluded from both contracts. It isn't a flat category
  vocabulary: agency code `1` is the Federal Ministry of Finance under one donor and the Ministry
  of Foreign Affairs under another, so its row key is `(code, activation_date, donor-code)`, a
  different shape from every other codelist in this set. If it's ever supported, it needs its own
  function and its own row grain — that's a separate decision, not something the category contract
  can absorb.

Empty `codelist_ids`, an id outside the supported set, or a duplicate all raise
`CodelistValidationError` before any request is made, same as the area contract.

### The category frame

`snapshot.frame` has **eleven** columns (the area frame has ten) — `string[pyarrow]` throughout,
except `activation_date`, which is `date32[pyarrow]`. Missing values are `pd.NA`, never an empty
string. `codelist_id`, `code`, `label`, `status`, `crs` and `tossd` are never null; everything else
is nullable.

| Column            | Source field                     | Null when                                                                     |
| ----------------- | -------------------------------- | ----------------------------------------------------------------------------- |
| `codelist_id`     | request parameter                | never                                                                         |
| `code`            | `code`                           | never                                                                         |
| `label`           | `name.narrative`, English        | never                                                                         |
| `status`          | `status`, lowercased             | never                                                                         |
| `crs`             | `crs` (`"0"`/`"1"`)              | never observed                                                                |
| `tossd`           | `tossd` (`"0"`/`"1"`)            | never observed                                                                |
| `activation_date` | `activation-date`                | 7 of the 23 codelists carry it on no row at all; 14–100% within the other 16  |
| `description`     | `description.narrative`, English | 13 of the 23 codelists carry it on no row at all; 14–100% within the other 10 |
| `category`        | `category`                       | 19 of the 23 codelists carry it on no row at all; 74–100% within the other 4  |
| `parent_code`     | `parent-code`                    | present in exactly one codelist (Purpose code); see below                     |
| `dac_reference`   | `dac:reference`                  | 13 of the 23 codelists carry it on no row at all; 14–100% within the other 10 |

Same rules as the area frame: `status` is lowercased at the boundary and constrained to
`active` / `withdrawn` / `future` / `heading`, with anything outside that domain passed through
unchanged and listed in `unknown_statuses`; `label` is the narrative with no `xml:lang` attribute,
falling back to the first entry if none matches.

Be honest about which columns are sparse: `category` and `parent_code` each live in a handful of
codelists — `category` in four, `parent_code` in exactly one. If your codelist isn't one of those,
the column is `pd.NA` for every row you fetch, not an error.

### The row grain: a five-column key

**One row per `(codelist_id, code, activation_date, crs, tossd)` — not one row per
`(codelist_id, code, activation_date)`.** This is the category contract's version of the area
contract's row-grain surprise above, and it bites the same way: a join that assumes one row per
code, or even one row per `(code, activation_date)`, is silently wrong rather than loudly broken.

The area contract's grain surprise is validity periods — a code's history over time. The category
contract's is different in kind: **the same code can mean two different things depending on which
OECD reporting standard is asking**, and a single fetch returns both meanings as separate rows,
distinguished only by the `crs`/`tossd` flags. Four codelists exhibit this — Purpose code, Channel
of delivery, Type of finance, Co-operation modality — the four richest and most-used codelists in
the category set.

Purpose code (`10`), code `15190`, verified against the shipped fixtures:

```
codelist_id  code   crs   tossd   activation_date
10           15190  0     1       <NA>
10           15190  1     0       <NA>
```

Same code, two rows. The `description` differs too — subtly, in wording only ("provider country"
vs. "donor country") — but `crs`/`tossd` is what tells you which reporting standard each row
belongs to.

Channel of delivery (`3`), code `11000`, where the difference is not subtle at all:

```
codelist_id  code   crs   tossd   label
3            11000  1     0       Donor Government
3            11000  0     1       Provider Government
```

Under CRS reporting, `11000` is "Donor Government." Under TOSSD reporting, the same code is
"Provider Government." Filtering on `code == "11000"` without also filtering on `crs`/`tossd`
mixes two different labels for what your pipeline will treat as one thing.

There is no `standard=` request parameter to narrow this to a three-column key. One request
already returns both standards' rows — asking for only one would mean **discarding** data we
already fetched, which is the same call the area contract made for `status` (fetch all four
statuses, always, and let the consumer filter). A row-level flag is filterable with
`frame[frame["crs"] == "1"]`; a request parameter that silently drops rows before you see them is
not.

`(codelist_id, code, activation_date, crs, tossd)` is unique across all 23 supported codelists —
verified directly against the live source, not assumed.

### `parent_code`: the CRS sector hierarchy

`parent_code` appears in exactly one codelist, Purpose code, and encodes the CRS sector hierarchy:
code `111`'s `parent_code` is `110`, and `110` exists elsewhere in the same codelist as a heading
row. Every non-empty parent reference resolves to a real code in the same codelist — verified
directly, not assumed.

It's included as a deliberate carve-out from the inclusion rule the rest of the frame follows (a
field earns a column by being present on a majority of rows in a codelist that ships). By that
rule alone, `parent_code` doesn't qualify: it fails majority coverage everywhere it appears. It's
in anyway because a coverage rule is a proxy for usefulness, and here the proxy is wrong — this is
the single most useful structural field on the most-used codelist in the set, not a stray one-off.

**Normalisation, worth stating explicitly because it's easy to get wrong:** the source sends an
empty string, not a missing field, for a majority of the rows that carry the `parent-code` key at
all. Of Purpose code's 392 rows, 124 carry the `parent-code` key — but 43 of those 124 carry it as
`""`, not a real reference. Only **81 of 392** carry a genuine non-empty parent. Empty string
normalises to `pd.NA`, so `frame["parent_code"].notna()` gives you 81, not 124 — a consumer
counting key-presence instead of genuine values will overcount by more than half. This was tested
explicitly against the shipped fixtures (verified above: 81 non-null rows on the Purpose code
subset of the frame).

### `dac_reference`: opaque provenance text

`dac_reference` (from the source's namespaced `dac:reference` field) is present in 10 of the 23
codelists, at 14–100% coverage within them. It clears the inclusion rule comfortably where it
appears — 100% on every row of PSI additionality and Type of Blended Finance, for instance.

Document it for what it is: **free-text document citation, not a controlled vocabulary.** Real
values look like this:

```
DCD/DAC/STAT(2015)23
DCD/DAC/STAT(2009)1;DCD/DAC/STAT(2018)42;DCD/DAC/STAT(2021)12/ADD1/REV1
Renamed from PDGG: DCD/DAC/STAT(2021)15/REV1
```

Sometimes one citation, sometimes several semicolon-joined, sometimes annotated with inline
changelog prose about why or when a code changed. It is not filterable or joinable in any
structured way, and its format is not promised — do not write code that parses it, splits it on
`;`, or treats it as an identifier. It's provenance you can read, closer to a footnote than a
category.

### `reconcile` does not accept category frames

`reconcile` hardcodes the area contract's three-column key and ten-column shape. A category
snapshot is rejected with `CodelistValidationError` rather than silently mis-keyed:

```python
from oda_reader.codelists import fetch_code_categories, reconcile
from oda_reader import CodelistValidationError

snapshot = fetch_code_categories(codelist_ids=("21",))
try:
    reconcile(previous=None, current=snapshot)
except CodelistValidationError as exc:
    print(exc)
# Codelist validation error: reconcile only supports the area contract's ten-column frame
# (...); got columns (...). The category contract (fetch_code_categories /
# parse_code_categories) is not supported by reconcile.
```

If you want history over category codelists — which codes were added, which were retired, what
changed — `reconcile` isn't it, at least not yet. Do what the area contract's own docs already
recommend as the fallback for when we're not there for you: keep your own dated `raw` snapshots
and diff the frames yourself. `parse_code_categories` makes those snapshots exactly as replayable
as the area contract's are — every guarantee on `CodelistSnapshot` (`content_hash`, `raw`, byte
parity) holds for a category snapshot the same way it holds for an area one.

## Exceptions

Five exception classes, all importable from `oda_reader` directly. They're shared by both
contracts — `fetch_code_categories` and `parse_code_categories` raise from the same five classes,
for the same reasons, as `fetch_codelists` and `parse_codelists`:

| Failure                                                                          | Exception                 | `is_retryable` |
| -------------------------------------------------------------------------------- | ------------------------- | -------------- |
| Network timeout or reset, HTTP 5xx/429/408/425, exhausted after internal retries | `CodelistFetchError`      | `True`         |
| HTTP 404/403/other 4xx, a cross-origin redirect                                  | `CodelistSourceError`     | `False`        |
| A 200 response that isn't the OECD codelist page                                 | `CodelistSourceError`     | `False`        |
| A required token missing from the handshake page                                 | `CodelistShapeError`      | `False`        |
| The JSON envelope doesn't have the shape we expect                               | `CodelistShapeError`      | `False`        |
| A row is missing a required field, or conflicts with another row sharing its key | `CodelistShapeError`      | `False`        |
| A codelist returned zero rows                                                    | `CodelistValidationError` | `False`        |
| `codelist_ids` is empty, unsupported, or has a duplicate                         | `CodelistValidationError` | `False`        |

`is_retryable` is `True` on exactly `CodelistFetchError` — a class attribute, so you can branch on
it without knowing the full subclass list:

```python
from oda_reader import CodelistError

try:
    snapshot = fetch_codelists()
except CodelistError as exc:
    if exc.is_retryable:
        ...  # transient — worth retrying
    else:
        raise  # source moved, or the page changed shape — needs a human
```

All five inherit from `CodelistError`, and only from `Exception` — none of them subclass
`ConnectionError` or `OSError`, so a broad `except OSError` around a fetch and a file write won't
accidentally swallow a codelist failure as a disk problem.

One redirect note, since it affects which exception you see: the OECD app answers a codelist
selection with an ordinary same-origin redirect, and `oda_reader` follows it automatically — that
part of the handshake is not an error. A redirect to a *different* host is refused and raises
`CodelistSourceError`, since that means we've been bounced somewhere that isn't the OECD app.

## Being a good citizen

Publishing this as a supported API changes the traffic picture: instead of one internal script
hitting OECD's server on a schedule, every consumer of `oda_reader.codelists` does. Three things
follow — for both contracts alike, since the rate limiter below is scoped to the OECD host, not to
which function you called.

**We identify ourselves.** Every request carries a `User-Agent` naming the library, its version,
and a contact URL: `oda-reader/<version> (+https://github.com/ONEcampaign/oda_reader)`. If our
traffic becomes a problem for OECD, they can find out who to email.

**Fetch at most a few times a day.** The recommended minimum interval is one fetch per hour per
process, and the intended cadence is daily at most — these codelists change a handful of times a
year. `oda_reader`'s package-wide rate limiter enforces a short-term floor on requests to the OECD
host, but it can't enforce your own scheduler; the interval above is advice you need to apply
yourself.

**Cache the snapshot. Don't fetch per run.** If your pipeline calls `fetch_codelists` inside a
per-partition loop, that's a bug, not a feature — fetch once, persist the result (`snapshot.raw`
if you want the exact bytes, the frame or a reconciled table if you want the data), and read from
that on every subsequent run within the day. `parse_codelists` exists partly so replaying a stored
payload is exactly as easy as fetching a new one.

## Support policy

Design decisions on this page — the column set, the row grain, the exception hierarchy — are a
contract, not an implementation detail, from the moment they ship. This section says plainly what
we promise, what we don't, and what happens when OECD changes something we can't control. It
covers both contracts; where a promise is specific to one, that's called out.

### What we promise

- The area contract's ten frame columns and the category contract's eleven, their names, their
  dtypes, and `pd.NA` as the null representation.
- One row per `(codelist_id, code, activation_date)` for the area contract; one row per
  `(codelist_id, code, activation_date, crs, tossd)` for the category contract.
- The `CodelistSnapshot` and `Reconciliation` attributes documented above — shared by both
  contracts, since both `fetch_codelists`/`parse_codelists` and `fetch_code_categories`/
  `parse_code_categories` return a `CodelistSnapshot`.
- That `label` carries the English label, on both contracts.
- That `content_hash` is version-prefixed and comparable between two snapshots built the same way.
- Which of the five exception types each failure raises, and that `is_retryable` is `True` on
  exactly one of them — identical taxonomy for both contracts.
- That `parse_codelists` and `parse_code_categories` each build the same snapshot from bytes, with
  no network access, so your own stored payloads keep working when the live page doesn't.
- That `Reconciliation.table` is a superset of `previous` by `(codelist_id, code, activation_date)`, that retirement is scoped to the codelists actually requested, and that
  consumer-added columns survive with a reappearing code keeping its original `first_seen` — for
  the area contract, which is all `reconcile` supports. It raises `CodelistValidationError` on a
  category-contract frame rather than silently mis-keying it.
- That `import oda_reader` opens no socket and resolves no hostname.
- Semantic versioning against all of the above.

### What we do not promise

- The content of OECD's codelists. We relay what OECD publishes; we do not curate it.
- That a label's text is stable, or that a code keeps the same `dotstat_code`.
- That the OECD page stays available, or that fetching keeps working.
- Anything about the structure of `snapshot.raw` — it is exactly what OECD sent.
- Response latency, or that a fetch succeeds on the first attempt.
- French labels, or any field only reachable through `snapshot.raw`.
- Codelists other than the ones listed as supported by each contract.
- Which narrative element we pick when locating the English label — the language is the promise,
  the selector is an implementation detail.
- That `dac_reference`'s format is stable, or that it's filterable, joinable, or parseable in any
  structured way — it's opaque provenance text (see above), not a controlled vocabulary.
- `reconcile` support for the category contract. Keep your own dated `raw` snapshots and diff them
  yourself if you need history over category codelists.

### Deprecation cadence

A deprecated name warns with `DeprecationWarning` for at least two minor releases or six months,
whichever is longer, and is removed only in the next major version. Every deprecation gets a
changelog entry naming the replacement. A column dtype change is a deprecation, not a silent fix,
and gets the same treatment wherever it's technically possible to warn.

### Provisional until a fixed date

`oda_reader.codelists` ships **documented-provisional**: until the earlier of a future `1.x`
release or **2026-12-31**, breaking changes to the frame's shape are still possible and will be
announced in the changelog rather than held to the deprecation cadence above. Once that window
closes, the surface is stable and everything in this policy applies without exception. There is no
runtime warning during the provisional window — a warning in a pipeline log is noise you can't act
on and will end up filtering, so the date and the changelog are the notice.

The category contract ships under this same window, on the same dated boundary — it isn't a
separate clock starting from when it shipped. It's a newer part of the same provisional surface.

### When OECD breaks the page

1. **You are not blocked on us.** This is first because it changes the shape of everything below
   it. Keep your own dated raw snapshots and your last good reconciled table; replaying stored
   payloads through `parse_codelists` keeps your pipeline running on "the last payload we have,
   plus an alert" while we work on a fix, with no release from us on your critical path.
1. `CodelistSourceError` or `CodelistShapeError` is raised, naming the stage that failed and, where
   we received a response body, carrying it (bounded and redacted) on the exception.
1. Triage starts from the exception class: `CodelistSourceError` means the page moved or is
   blocking us, `CodelistShapeError` means the parser needs a human, `CodelistFetchError` means
   transient and already retried.
1. Our target for acknowledging a report is **two working days**. That is a best-effort target
   from a small maintainer team, stated plainly here — **it is not an SLA**.
