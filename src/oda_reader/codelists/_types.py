"""Shared constants and return types for the oda_reader.codelists package.

Holds the constants ``_fetch.py`` and ``_parse.py`` both need, plus
``CodelistSnapshot`` and ``Reconciliation``. Importing ``pandas`` here is
fine -- the thing this module avoids is importing ``_fetch``, which would
drag ``requests`` into the parse path.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

import pandas as pd

DEFAULT_URL = "https://development-finance-codelists.oecd.org/CodesList.aspx"

# The two area codelists this package is prepared to fetch and parse.
SUPPORTED_CODELIST_IDS: tuple[str, ...] = ("5", "13")

# The 23 flat non-area codelists the category contract (fetch_code_categories /
# parse_code_categories) is prepared to fetch and parse -- every OECD codelist
# except Provider (5) and Recipient (13), which the area contract owns, and
# Provider agency (16), which the agency contract (fetch_provider_agencies /
# parse_provider_agencies) owns instead. An agency code is only meaningful
# within its donor: code 1 is the Federal Ministry of Finance under one donor
# and Foreign Affairs under another, and 99 of its 110 codes carry more than
# one label across donors -- a shape neither this contract's flat vocabulary
# nor the area contract's (code, activation_date) grain can express.
SUPPORTED_CATEGORY_IDS: tuple[str, ...] = (
    "1",
    "2",
    "3",
    "4",
    "6",
    "7",
    "8",
    "9",
    "10",
    "11",
    "12",
    "14",
    "15",
    "17",
    "18",
    "19",
    "20",
    "21",
    "22",
    "23",
    "24",
    "25",
    "26",
)

# The one OECD codelist the agency contract (fetch_provider_agencies /
# parse_provider_agencies) is prepared to fetch and parse -- a tuple of
# length one, uniform with its two siblings above, not a bare str: generic
# code walking all three id-vocabularies (e.g. `for ids in (SUPPORTED_CODELIST_IDS,
# SUPPORTED_CATEGORY_IDS, SUPPORTED_AGENCY_IDS): ...`) would otherwise iterate
# a bare "16" character-by-character into "1" and "6", the exact mistake the
# codelist_ids validators exist to catch -- a constant has no validator.
# Provider agency is the only entity keyed on (donor-code, code), so a caller
# could never legally pass anything but this one id; that is also why
# fetch_provider_agencies takes no codelist_ids parameter at all.
SUPPORTED_AGENCY_IDS: tuple[str, ...] = ("16",)

# The documented status domain a codelist row's `status` field may carry,
# lowercase-normalised. Codelist 5 returns "Active", codelist 13 returns
# "active" -- casing is normalised at the parse boundary. A row landing
# outside this domain is passed through unchanged rather than dropped, and
# surfaced via CodelistSnapshot.unknown_statuses:
# dropping data we don't recognise is worse than passing it on.
STATUS_DOMAIN: tuple[str, ...] = ("active", "withdrawn", "future", "heading")

# ASP.NET postback control IDs, reverse-engineered from view-source -- OECD
# publishes no documentation for CodesList.aspx. The four status checkboxes:
#   Cblstatus$0 = Active
#   Cblstatus$1 = Future
#   Cblstatus$2 = Heading
#   Cblstatus$3 = Withdrawn
# Which of these a given request sends is a decision made where the POST
# body is built (see _fetch.py's _deliberate_controls), not here.

# The four columns reconcile adds on top of a snapshot's ten contract
# columns to build Reconciliation.table. Reserved names: a `previous` frame
# carrying one of these must carry a compatible value, not an unrelated
# consumer column that happens to share the name.
LINEAGE_COLUMNS: tuple[str, ...] = (
    "first_seen",
    "last_seen",
    "source_status",
    "presence",
)

# Reconciliation.table's `presence` column takes exactly one of these two
# values, never pd.NA -- even for a row backfilled from a `previous` with
# no lineage columns at all, where it defaults to "current" absent
# contrary evidence (see _reconcile.py).
PRESENCE_DOMAIN: tuple[str, ...] = ("current", "retired")


@dataclass(frozen=True, slots=True, kw_only=True, eq=False)
class CodelistSnapshot:
    """One fetch of one or more OECD DAC codelists.

    Returned by all three public contracts, in a fetch/parse pair each:
    ``fetch_codelists``/``parse_codelists`` for the **area** codelists
    (providers and recipients), ``fetch_code_categories``/
    ``parse_code_categories`` for the 23 flat **category** codelists, and
    ``fetch_provider_agencies``/``parse_provider_agencies`` for the single
    **agency** codelist (Provider agency, 16). The ``fetch_*`` half performs
    the live ASPX handshake; the ``parse_*`` half builds the same thing from
    bytes already in hand, with no network access -- so every guarantee below
    still holds on a payload obtained any other way (yesterday's snapshot, a
    colleague's manual download) when OECD breaks the live page.

    One type serves all three because every guarantee here -- replayable
    ``raw``, ``content_hash``, ``unknown_statuses``, the support policy -- is
    identical across them; only ``frame``'s columns and row grain differ.
    ``contract`` records which pair built it, so code that receives a
    snapshot second-hand can tell them apart without inspecting
    ``frame.columns``.

    ``frozen=True`` prevents *rebinding* an attribute (``snapshot.frame =
    ...``); it does not prevent mutating the DataFrame or the mapping behind
    ``frame`` and ``raw`` in place. ``raw`` is wrapped in
    ``MappingProxyType`` by ``parse_codelists`` so at least the mapping
    itself can't be edited.

    ``eq=False`` is deliberate, not an oversight: a generated ``__eq__``
    over a ``pd.DataFrame`` returns an array and raises on ``bool()``, and a
    generated ``__hash__`` over a ``Mapping`` is not hashable. With
    ``eq=False``, ``==`` falls back to identity comparison, so two logically
    identical snapshots compare unequal. **Compare snapshots by
    ``.content_hash``, not ``==``.**

    Attributes:
        contract: Which contract built this snapshot -- ``"area"`` from
            ``fetch_codelists``/``parse_codelists``, ``"category"`` from
            ``fetch_code_categories``/``parse_code_categories``, or
            ``"agency"`` from ``fetch_provider_agencies``/
            ``parse_provider_agencies``. Determines ``frame``'s columns and
            row grain, and is what ``reconcile`` checks to reject a frame it
            cannot key. Required rather than defaulted: there is no value
            that would be right for all three, and a snapshot rebuilt by
            hand from stored ``raw`` has to say which it is.
        frame: The contract depends on which pair built this snapshot.
            **Area**: ten columns, one row per ``(codelist_id, code,
            activation_date)`` -- see ``parse_codelists``. **Category**:
            eleven columns, keyed additionally on ``crs`` and ``tossd``
            because the same code can carry different definitions under the
            two standards -- see ``parse_code_categories``. **Agency**:
            twelve columns, one row per ``(codelist_id, donor_code, code,
            activation_date)`` -- an agency code is only meaningful within
            its donor, so ``donor_code`` joins the key rather than ``crs``/
            ``tossd``, which agency rows do not split by -- see
            ``parse_provider_agencies``. Each function's docstring states
            its own contract in full.
        raw: ``codelist_id -> the exact response bytes for that codelist``,
            as OECD sent them, unmodified. A ``MappingProxyType``.
        fetched_at: When the underlying bytes were obtained. Timezone-aware.
        source_url: Where the bytes came from.
        codelist_ids: The codelists this snapshot covers, in the order they
            were requested.
        content_hash: ``"v1:"`` followed by a sha256 hex digest of the
            frame's contract columns. Two snapshots with equal
            ``content_hash`` have equal frame content; hashes do not compare
            across ``"v1:"``/``"v2:"`` prefix versions.
        unknown_statuses: Distinct ``status`` values seen outside the
            documented domain (``active``, ``withdrawn``, ``future``,
            ``heading``), in first-seen order. Empty in the normal case;
            affected rows stay in ``frame`` with the value intact rather
            than being dropped -- dropping data we don't recognise is worse
            than passing it on.
    """

    contract: Literal["area", "category", "agency"]
    frame: pd.DataFrame
    raw: Mapping[str, bytes]
    fetched_at: datetime
    source_url: str
    codelist_ids: tuple[str, ...]
    content_hash: str
    unknown_statuses: tuple[str, ...]


@dataclass(frozen=True, slots=True, kw_only=True, eq=False)
class Reconciliation:
    """The result of merging a previous codelist table with a fresh snapshot.

    Built by ``reconcile``. ``table`` is a superset of ``previous`` by
    ``(codelist_id, code, activation_date)`` -- no input row is ever
    dropped, so a code OECD stops listing survives with ``presence="retired"``
    rather than disappearing from a ``CREATE OR REPLACE`` overwrite.

    ``eq=False`` for the same reason as ``CodelistSnapshot``: a generated
    ``__eq__`` over the four ``pd.DataFrame`` fields would return arrays and
    raise on ``bool()``. Do not use ``==`` to ask "did anything change" --
    that question is what ``is_unchanged`` answers directly. Compare
    snapshots by ``.content_hash``, not ``==``: if you're asking whether
    two runs saw the same OECD data, compare the ``content_hash`` of the
    ``CodelistSnapshot`` you passed in as ``current``, not the
    ``Reconciliation`` objects those runs produced.

    Attributes:
        table: The union, ready to load: ``previous`` plus every row from
            ``current``, with ``pd.NA``-safe never-delete semantics. Carries
            the ten contract columns, the four lineage columns
            (``first_seen``, ``last_seen``, ``source_status``,
            ``presence``), and any extra column ``previous`` had.
        added: Rows in ``current`` that were not in ``previous`` at all --
            a genuinely new key. Full ``table`` column set, not a key
            subset, so it can be routed straight into an alert or an audit
            table without a join back.
        retired: Rows that transitioned to ``presence="retired"`` *this
            run* -- i.e. newly retired, not rows that were already retired
            and remain absent. Full ``table`` column set.
        changed: One row per changed value: a non-key contract column that
            differs between ``previous`` and ``current`` for a row present
            in both, or a code reappearing after retirement (reported here
            with ``column="presence"``, never in ``added`` -- see
            ``reconcile``'s docstring). Columns: ``codelist_id``, ``code``,
            ``activation_date``, ``column``, ``old_value``, ``new_value``,
            ``observed_at``.
        is_unchanged: True when ``added``, ``retired`` and ``changed`` are
            all empty. Advancing ``last_seen`` on an otherwise-identical
            row is routine and does *not* make this False -- if it did,
            ``is_unchanged`` would be False on every run and mean nothing.
    """

    table: pd.DataFrame
    added: pd.DataFrame
    retired: pd.DataFrame
    changed: pd.DataFrame
    is_unchanged: bool
