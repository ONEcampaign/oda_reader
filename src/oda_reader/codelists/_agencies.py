"""The agency contract: fetch and parse for OECD codelist 16 (Provider agency).

Parallels ``_categories.py``'s "a new contract on top of shared machinery"
shape, over exactly one codelist rather than 23. ``fetch_provider_agencies``,
at the bottom of this module, is the public entry point:
``parse_provider_agencies`` composed with the handshake ``_fetch.py`` already
owns.

**Why this is a third contract, not a category.** An agency code is only
meaningful within its donor -- code ``"1"`` alone means the Federal Ministry
of Finance under one donor and Foreign Affairs under another. Measured
against the reference payload: ``(code, donor-code)`` is unique across all
1,374 rows, but 99 of the 110 distinct codes carry more than one distinct
label across donors, and code ``"1"`` alone carries 185. Neither the area
contract's ``(code, activation_date)`` grain nor the category contract's flat
per-code vocabulary can express that, so ``donor_code`` joins the row key
here instead -- see ``_AGENCY_KEY_COLUMNS``.

Everything below the frame is reused, not reimplemented: the ASPX handshake
(``_fetch_codelist_bytes``), the retry loop, the exception taxonomy, the
envelope validator, the row-level field validators, the English-narrative
selector, the activation-date validator, the frame builder and
``content_hash``'s canonicalisation are all imported from ``_fetch.py`` /
``_parse.py`` and parameterised for this contract's twelve columns rather
than copied.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from functools import partial
from typing import Any

from oda_reader.codelists._fetch import _fetch_codelist_bytes
from oda_reader.codelists._parse import (
    _parse_activation_date,
    _require_code,
    _require_label,
    _require_row_field,
    _select_english_label,
    _snapshot_from_raw,
)
from oda_reader.codelists._types import (
    DEFAULT_URL,
    STATUS_DOMAIN,
    SUPPORTED_AGENCY_IDS,
    CodelistSnapshot,
)
from oda_reader.exceptions import CodelistShapeError, CodelistValidationError

# The twelve agency-contract columns, in the documented order. content_hash's
# canonicalisation walks this exact order, same as the area contract's
# _CONTRACT_COLUMNS and the category contract's _CATEGORY_CONTRACT_COLUMNS.
_AGENCY_CONTRACT_COLUMNS: tuple[str, ...] = (
    "codelist_id",
    "donor_code",
    "code",
    "label",
    "status",
    "acronym",
    "agency_type_code",
    "agency_type",
    "crs",
    "tossd",
    "used_in_cpa",
    "activation_date",
)

# donor_code leads the key, ahead of code, because it is what makes code
# meaningful in the first place -- see the module docstring. activation_date
# stays in the key even though it is pd.NA on every row today: it costs
# nothing and covers OECD adding validity periods later, exactly as the area
# contract keeps the column while codelist 5 carries no dates either.
#
# crs/tossd stay OUT of the key, unlike the category contract's
# _CATEGORY_KEY_COLUMNS: there, the same code can carry a different
# definition under CRS and TOSSD reporting, so both standards' rows must
# coexist. Agency rows do not split by reporting standard -- if OECD ever
# makes them, two rows sharing this key with different crs/tossd values
# raise a conflicting-value error in _project_agency_rows rather than being
# silently mis-keyed.
_AGENCY_KEY_COLUMNS: tuple[str, ...] = (
    "codelist_id",
    "donor_code",
    "code",
    "activation_date",
)


def _require_agency_type(
    row: dict[str, Any],
    *,
    code: str,
    body: bytes,
    source_url: str,
    codelist_id: str,
) -> tuple[str, str]:
    """The row's nested ``Agencytype.code`` and English ``Agencytype.name`` label.

    No existing projector reaches two levels down before this one, so every
    step is guarded with ``isinstance`` rather than trusted. ``Agencytype``
    is present on all 1,374 rows of the reference payload, and its ``code``
    and derivable ``name`` are as load-bearing as ``crs``/``tossd`` -- so a
    missing or malformed ``Agencytype`` is treated as required, matching
    those two, not as nullable like ``acronym``/``used_in_cpa``: it raises
    ``CodelistShapeError(stage="row")`` rather than yielding nulls. A payload
    shape change here is exactly the kind of thing that error exists to
    surface loudly, and every agency row observed so far carries it.
    """
    agency_type = row.get("Agencytype")
    if not isinstance(agency_type, dict):
        raise CodelistShapeError(
            stage="row",
            detail=f"code {code!r} has no 'Agencytype' object",
            body=body,
            url=source_url,
            codelist_id=codelist_id,
        )
    agency_type_code = agency_type.get("code")
    if not isinstance(agency_type_code, str) or not agency_type_code:
        raise CodelistShapeError(
            stage="row",
            detail=f"code {code!r} has no 'Agencytype.code' field",
            body=body,
            url=source_url,
            codelist_id=codelist_id,
        )
    agency_type_label = _select_english_label(agency_type.get("name"))
    if agency_type_label is None:
        raise CodelistShapeError(
            stage="row",
            detail=f"code {code!r} has no derivable 'Agencytype.name' label",
            body=body,
            url=source_url,
            codelist_id=codelist_id,
        )
    return agency_type_code, agency_type_label


def _project_agency_rows(
    *, codelist_id: str, rows: list[dict[str, Any]], body: bytes, source_url: str
) -> tuple[list[dict[str, Any]], list[str]]:
    """Project *rows* onto the twelve agency-contract columns for one codelist.

    Mirrors ``_categories.py``'s ``_project_category_rows``, with the
    differences this contract's shape demands: the row-dedup key is
    ``(donor_code, code, activation_date)`` -- donor-first, because an
    agency code is only meaningful within its donor -- and five source
    fields are projected (``donor-code``, ``acronym``, the nested
    ``Agencytype.code``/``Agencytype.name``, ``UsedinCPA``) in place of the
    category contract's ``description``/``category``/``parent-code``/
    ``dac:reference``.

    Returns the projected records (one per unique ``(donor_code, code,
    activation_date)`` key) and the distinct out-of-domain ``status`` values
    seen, in first-seen order.

    A row missing ``code``, ``donor-code``, a derivable label, ``status``,
    ``crs``, ``tossd``, or a well-formed ``Agencytype``, or carrying a
    malformed ``activation_date``, raises ``CodelistShapeError(stage="row")``.
    Two rows sharing a full key with conflicting projected values also raise;
    two rows sharing the key with identical projected values are silently
    deduplicated. An empty *rows* raises ``CodelistValidationError``,
    matching both other contracts' treatment of a zero-row response as a
    self-consistency failure rather than a shape one.
    """
    if not rows:
        raise CodelistValidationError(
            detail=f"codelist {codelist_id!r} returned zero rows",
            codelist_id=codelist_id,
        )

    by_key: dict[tuple[str, str, str | None], dict[str, Any]] = {}
    order: list[tuple[str, str, str | None]] = []
    unknown_statuses: list[str] = []
    seen_unknown: set[str] = set()

    for row in rows:
        code = _require_code(
            row, body=body, source_url=source_url, codelist_id=codelist_id
        )
        # Every _require_row_field call below shares these four keyword
        # arguments, differing only in which field they require -- bound
        # once per row rather than repeated at each call site.
        require_field = partial(
            _require_row_field,
            row,
            code=code,
            body=body,
            source_url=source_url,
            codelist_id=codelist_id,
        )
        donor_code = require_field("donor-code")
        label = _require_label(
            row, code=code, body=body, source_url=source_url, codelist_id=codelist_id
        )

        # A property of this validity period, not of the agency as a whole
        # -- same reasoning as the other two contracts' status handling.
        status = require_field("status").lower()
        if status not in STATUS_DOMAIN and status not in seen_unknown:
            seen_unknown.add(status)
            unknown_statuses.append(status)

        agency_type_code, agency_type = _require_agency_type(
            row,
            code=code,
            body=body,
            source_url=source_url,
            codelist_id=codelist_id,
        )

        crs = require_field("crs")
        tossd = require_field("tossd")

        activation_date = _parse_activation_date(
            row.get("activation-date"),
            code=code,
            body=body,
            source_url=source_url,
            codelist_id=codelist_id,
        )

        record = {
            "codelist_id": codelist_id,
            "donor_code": donor_code,
            "code": code,
            "label": label,
            "status": status,
            "acronym": _select_english_label(row.get("acronym")),
            "agency_type_code": agency_type_code,
            "agency_type": agency_type,
            "crs": crs,
            "tossd": tossd,
            "used_in_cpa": row.get("UsedinCPA"),
            "activation_date": activation_date,
        }

        # donor_code leads the key -- see the module docstring and
        # _AGENCY_KEY_COLUMNS. `None == None` compares correctly in a
        # Python dict, so this is NA-safe by construction, same as the area
        # contract's key.
        key = (donor_code, code, activation_date)
        existing = by_key.get(key)
        if existing is None:
            by_key[key] = record
            order.append(key)
        elif existing != record:
            raise CodelistShapeError(
                stage="row",
                detail=(
                    f"donor_code {donor_code!r} code {code!r} activation_date "
                    f"{activation_date!r} appears twice with conflicting values"
                ),
                body=body,
                url=source_url,
                codelist_id=codelist_id,
            )
        # else: a byte-identical duplicate key -- dropped silently, matching
        # the other two contracts.

    return [by_key[key] for key in order], unknown_statuses


def parse_provider_agencies(
    *,
    raw: Mapping[str, bytes],
    fetched_at: datetime,
    source_url: str = DEFAULT_URL,
) -> CodelistSnapshot:
    """Build a CodelistSnapshot from bytes already in hand, for the agency contract.

    The agency-contract counterpart of ``parse_codelists`` and
    ``parse_code_categories``: the same no-network escape hatch, over
    codelist 16 (Provider agency) rather than the area or category
    codelists. Every guarantee documented on ``CodelistSnapshot`` holds here
    too -- ``content_hash``, ``raw`` replayability, ``unknown_statuses`` --
    just computed over the twelve-column agency frame rather than the other
    two contracts' ten and eleven.

    Args:
        raw: ``codelist_id -> the exact response bytes`` for that codelist.
            In practice this is a single-entry mapping keyed on the one id
            in ``SUPPORTED_AGENCY_IDS``, since there is only one agency
            codelist, though this function itself does not validate that --
            that is ``fetch_provider_agencies``'s job, before any network
            request is made; a caller building ``raw`` by hand (e.g.
            replaying a stored payload) is trusted to know what they are
            replaying.
        fetched_at: When *raw* was obtained. Must be timezone-aware, for the
            same reason as ``parse_codelists``.
        source_url: Recorded on the returned snapshot; does not affect
            parsing.

    Returns:
        A ``CodelistSnapshot`` whose ``frame`` holds one row per
        ``(codelist_id, donor_code, code, activation_date)``. An agency code
        that means something different under two donors carries one row per
        donor rather than one row overall.

    Raises:
        CodelistShapeError: A payload is not JSON, its envelope is
            malformed, or a row is missing a required field, carries a
            malformed ``activation_date`` or a malformed ``Agencytype``, or
            conflicts with another row sharing its four-column key.
        CodelistValidationError: *raw* is empty, a codelist in *raw* produced
            zero rows, or *fetched_at* is a naive datetime.
    """
    return _snapshot_from_raw(
        raw=raw,
        fetched_at=fetched_at,
        source_url=source_url,
        contract="agency",
        columns=_AGENCY_CONTRACT_COLUMNS,
        key_columns=_AGENCY_KEY_COLUMNS,
        project_rows=_project_agency_rows,
    )


def fetch_provider_agencies(
    *,
    timeout: int = 30,
    retries: int = 2,
) -> CodelistSnapshot:
    """Fetch and parse OECD codelist 16 (Provider agency) via the live ASPX handshake.

    The agency-contract counterpart of ``fetch_codelists`` and
    ``fetch_code_categories``. ``parse_provider_agencies`` is composed with
    the same handshake ``fetch_codelists`` uses; every guarantee documented
    there holds here too.

    Unlike ``fetch_codelists``/``fetch_code_categories``, there is no
    ``codelist_ids`` parameter: codelist 16 is the only agency codelist, so
    it would have exactly one legal value -- a parameter that can only ever
    be passed one way is not a parameter worth having.

    Args:
        timeout: Per-request timeout in seconds, applied to every request of
            the handshake.
        retries: Additional attempts after the first, so total attempts is
            ``retries + 1``.

    Returns:
        A ``CodelistSnapshot`` covering codelist 16, with the twelve-column
        agency frame.

    Raises:
        CodelistFetchError: Transport failures or 5xx/429/408/425 persisted
            through every attempt.
        CodelistSourceError: A 4xx, an unfollowed redirect, or a 200
            response that is not the CodesList.aspx page.
        CodelistShapeError: A required hidden token was absent or empty, or
            the downloaded payload's shape did not match what
            ``parse_provider_agencies`` expects.
        CodelistValidationError: The payload parsed to zero rows, or
            *fetched_at* is a naive datetime.
    """
    (agency_id,) = SUPPORTED_AGENCY_IDS
    raw: dict[str, bytes] = {
        agency_id: _fetch_codelist_bytes(agency_id, timeout=timeout, retries=retries)
    }
    return parse_provider_agencies(
        raw=raw, fetched_at=datetime.now(UTC), source_url=DEFAULT_URL
    )
