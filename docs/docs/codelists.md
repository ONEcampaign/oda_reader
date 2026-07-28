# Codelists

`oda_reader.codelists` fetches the OECD DAC reference tables the data is coded against. A fetch
returns a `CodelistSnapshot` carrying a pandas DataFrame and the raw bytes it was parsed from.

Which function you call depends on the codelist. Provider and recipient codes come from
`fetch_codelists`. Purpose codes, channels of delivery, markers and twenty others come from
`fetch_code_categories`. Provider agencies come from `fetch_provider_agencies`.
[Codelist ID Reference](codelist-ids.md) maps all 26 ids to their contract.

## Getting a snapshot

`oda_reader.codelists` is not re-exported from `oda_reader`, so import the submodule:

```python
from oda_reader.codelists import fetch_codelists

snapshot = fetch_codelists()
snapshot.frame.head()
```

```text
codelist_id code   label status       type dotstat_code iso3 crs tossd activation_date
          5    1 Austria active DAC member          AUT  AUT   1     1            <NA>
          5    2 Belgium active DAC member          BEL  BEL   1     1            <NA>
          5    3 Denmark active DAC member          DNK  DNK   1     1            <NA>
          5    4  France active DAC member          FRA  FRA   1     1            <NA>
          5    5 Germany active DAC member          DEU  DEU   1     1            <NA>
```

A bare call fetches both area codelists, 577 rows as of July 2026.

Category codelists take an explicit list, since fetching all 23 pulls about 900 KB:

```python
from oda_reader.codelists import fetch_code_categories

purpose_codes = fetch_code_categories(codelist_ids=("10",)).frame
```

Pass a sequence even for one codelist. `codelist_ids="10"` raises `CodelistValidationError`.

Provider agencies take no argument, since codelist 16 is the only one:

```python
from oda_reader.codelists import fetch_provider_agencies

agencies = fetch_provider_agencies().frame
```

## Frame keys

Each contract keys its frame differently.

| Contract | Function                  | One row per                                        |
| -------- | ------------------------- | -------------------------------------------------- |
| Area     | `fetch_codelists`         | `(codelist_id, code, activation_date)`             |
| Category | `fetch_code_categories`   | `(codelist_id, code, activation_date, crs, tossd)` |
| Agency   | `fetch_provider_agencies` | `(codelist_id, donor_code, code, activation_date)` |

`snapshot.contract` says which pair built a snapshot: `"area"`, `"category"` or `"agency"`.

The same code appears in several codelists with different meanings, so include `codelist_id` in every
`groupby` and merge key.

### Area: one row per validity period

OECD publishes providers and recipients as validity periods, so a code whose status changed over time
carries one row per period. Codelist 13 returns 364 rows across 207 distinct codes. Code `130`
(Algeria) carries three:

```text
codelist_id code     label    status iso3 activation_date
         13  130   Algeria withdrawn  DZA      1996-01-01
         13  130   Algeria withdrawn  DZA      2011-01-01
         13  130   Algeria    active  DZA      2022-01-01
```

All three rows stay in the frame.

**Each row's `status` describes its own period.** A code's current state is its row with the greatest
`activation_date`:

```python
frame = fetch_codelists().frame

# ✅ One row per code, carrying its current state
latest = (
    frame.sort_values("activation_date")
    .groupby(["codelist_id", "code"], dropna=False)
    .tail(1)
)

# ❌ Drops every code whose latest period is withdrawn
active = frame[frame["status"] == "active"]
```

`latest` returns 420 rows, one per code. `active` returns 390, silently dropping the 30 codes that
are currently withdrawn.

**`pd.NA` is part of the key.** Codelist 5 carries no `activation_date` at all, so its 213 rows key
on a null third component. Pass `dropna=False` to any `groupby` on the key, since pandas drops
null-keyed rows by default.

### Category: one row per standard and period

Four codelists define the same code differently under OECD's two reporting standards, and a single
request returns both definitions. The `crs` and `tossd` columns say which standard a row belongs to.
Channel of delivery (`"3"`), code `11000`:

```text
codelist_id  code crs tossd               label
          3 11000   1     0    Donor Government
          3 11000   0     1 Provider Government
```

Purpose code, Channel of delivery, Type of finance and Co-operation modality all work this way.
Filter to the standard you report against:

```python
channels = fetch_code_categories(codelist_ids=("3",)).frame

crs_rows = channels[channels["crs"] == "1"]
```

There is no `standard=` parameter.

Category codes also carry validity periods, and those account for more repeated codes than the
standard split does. Channel of delivery returns 905 rows over 472 codes: 29 codes are split across
the two standards, while 280 carry more than one activation date. Filtering to one standard leaves
164 codes still holding several rows, so take the latest period as well when you need one row per
code.

### Agency: one row per donor

An agency code is only meaningful inside its donor. Code `1` is the Federal Ministry of Finance under
Austria and the Ministry of Foreign Affairs under Denmark:

```text
codelist_id donor_code code                                       label acronym
         16          1    1                 Federal Ministry of Finance     BMF
         16          3    1                 Ministry of Foreign Affairs     MFA
         16          4    1                                  Government     GOV
```

Codelist 16 returns 1,374 rows over 110 distinct agency codes across 198 donors, and code `1` alone
appears under 194 of them. Join on `donor_code` and `code` together.

## Reconciling against history

`fetch_codelists` gives you today's answer. `reconcile` merges it into the table you already have and
tells you what moved, so a code OECD stops listing survives your next overwrite instead of
disappearing from the table.

```python
import pandas as pd

from oda_reader.codelists import fetch_codelists, reconcile

previous = pd.read_parquet("dac_codelists.parquet")
rec = reconcile(previous=previous, current=fetch_codelists())

if not rec.is_unchanged:
    print(f"{len(rec.added)} added, {len(rec.retired)} retired, {len(rec.changed)} changed")

rec.table.to_parquet("dac_codelists.parquet")
```

Pass `previous=None` on the first run, before you have a stored table. Where `previous` comes from is
your choice. `reconcile` coerces contract columns to their contract dtypes on entry, so a table that
came back from parquet, a CSV, or a warehouse cursor as `int64`/`object` reconciles without manual
casting.

`rec.table` is a superset of what you passed in. A code that stopped being listed comes back with
`presence="retired"` and its last known values intact, plus lineage columns (`first_seen`,
`last_seen`, `source_status`, `presence`). Any extra columns your own table carried survive
untouched. The only way to lose a code is to omit `previous`.

`rec.added`, `rec.retired` and `rec.changed` are small frames meant to be read or alerted on.
`reconcile` doesn't decide whether a change matters, so thresholds and blocking checks belong in your
orchestrator. Three behaviours matter when you alert on them:

- **Retirement is scoped to the codelists in `current`.** Reconciling a codelist-5-only snapshot
  against a full table leaves codelist 13 rows at `presence="current"`.
- **A code that reappears is a change, not an addition.** It returns to `presence="current"`, keeps
  its original `first_seen`, and shows up in `rec.changed` with `column="presence"`.
- **`is_unchanged` ignores `last_seen`.** Advancing it on an otherwise identical row is not a change.

`reconcile` takes area snapshots only. For history over the other contracts, keep dated `raw`
snapshots and diff the frames yourself.

## When a fetch fails

Everything raises from `CodelistError`, and `is_retryable` tells you which branch you're on:

```python
from oda_reader import CodelistError
from oda_reader.codelists import fetch_codelists

try:
    snapshot = fetch_codelists()
except CodelistError as exc:
    if exc.is_retryable:
        ...  # transient
    else:
        raise  # the source moved or changed shape, a human needs to look
```

Every snapshot carries the exact bytes it was parsed from, so save them on every successful run:

```python
from pathlib import Path

for codelist_id, payload in snapshot.raw.items():
    Path(f"codelist_{codelist_id}.json").write_bytes(payload)
```

`parse_codelists` rebuilds a snapshot from those bytes with no network access, which keeps a pipeline
running while the OECD page is broken:

```python
import datetime
from pathlib import Path

from oda_reader.codelists import parse_codelists

raw = {
    "5": Path("codelist_5.json").read_bytes(),
    "13": Path("codelist_13.json").read_bytes(),
}
snapshot = parse_codelists(
    raw=raw,
    fetched_at=datetime.datetime(2026, 7, 20, tzinfo=datetime.UTC),
)
```

`fetched_at` must be timezone-aware, and `raw` must not be empty. `parse_code_categories` and
`parse_provider_agencies` do the same for their own payloads.

See the [exception reference](codelists-reference.md#exceptions) for which failure raises what.

## How often to fetch

These codelists change a handful of times a year. Fetch once a day at most, persist the result, and
read from that on subsequent runs. A `fetch_codelists` call inside a per-partition loop is a bug.

Requests to the OECD host go through the package-wide rate limiter. See
[Caching & Performance](caching.md) to configure it.

## Next

- **[Codelists Reference](codelists-reference.md)** - Signatures, frame columns, and the exception
  contract
- **[Codelist ID Reference](codelist-ids.md)** - All 26 OECD ids, their names, and which contract
  covers them
