"""The category contract: fetch and parse for the 23 flat non-area OECD DAC codelists.

Parallels ``_fetch.py`` + ``_parse.py``'s area contract in one module rather
than two, because it is a single bounded addition on top of machinery that
already exists there. ``fetch_code_categories``, at the bottom of this
module, is the public entry point: ``parse_code_categories`` composed with
the handshake ``_fetch.py`` already owns.

Everything below the frame is reused, not reimplemented: the ASPX handshake
(``_fetch_codelist_bytes``), the retry loop, the exception taxonomy, the
envelope validator, the row-level field validators, the English-narrative
selector, the activation-date validator, the frame builder and
``content_hash``'s canonicalisation are all imported from ``_fetch.py`` /
``_parse.py`` and parameterised for this contract's eleven columns rather
than copied.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
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
    SUPPORTED_CATEGORY_IDS,
    CodelistSnapshot,
)
from oda_reader.exceptions import CodelistShapeError, CodelistValidationError

# The eleven category-contract columns, in the documented order. content_hash's
# canonicalisation walks this exact order, same as the area contract's
# _CONTRACT_COLUMNS.
_CATEGORY_CONTRACT_COLUMNS: tuple[str, ...] = (
    "codelist_id",
    "code",
    "label",
    "status",
    "crs",
    "tossd",
    "activation_date",
    "description",
    "category",
    "parent_code",
    "dac_reference",
)

# The row grain adds crs/tossd to the area contract's three-column key
# because four codelists (Purpose code, Channel of delivery, Type of finance,
# Co-operation modality) carry the same code with a different meaning per
# reporting standard, distinguished only at the row level by crs/tossd -- one
# request already returns both standards' rows, so the key has to carry that
# distinction rather than a `standard=` parameter discarding it.
_CATEGORY_KEY_COLUMNS: tuple[str, ...] = (
    "codelist_id",
    "code",
    "activation_date",
    "crs",
    "tossd",
)


def _project_category_rows(
    *, codelist_id: str, rows: list[dict[str, Any]], body: bytes, source_url: str
) -> tuple[list[dict[str, Any]], list[str]]:
    """Project *rows* onto the eleven category-contract columns for one codelist.

    Mirrors ``_parse.py``'s ``_project_rows`` for the area contract, with the
    differences the category contract's shape demands: the row-dedup key adds
    ``crs``/``tossd``, and six source fields are projected
    (``crs``, ``tossd``, ``description``, ``category``, ``parent_code``,
    ``dac_reference``) in place of the area contract's
    ``type``/``dotstat_code``/``iso3``.

    Returns the projected records (one per unique
    ``(code, activation_date, crs, tossd)`` key) and the distinct
    out-of-domain ``status`` values seen, in first-seen order.

    A row missing ``code``, a derivable label, ``status``, ``crs`` or ``tossd``,
    or carrying a malformed ``activation_date``, raises
    ``CodelistShapeError(stage="row")``. Two rows sharing a full key with
    conflicting projected values also raise; two rows sharing the key with
    identical projected values are silently deduplicated. An empty *rows*
    raises ``CodelistValidationError``, matching the area contract's
    treatment of a zero-row response as a self-consistency failure rather
    than a shape one.
    """
    if not rows:
        raise CodelistValidationError(
            detail=f"codelist {codelist_id!r} returned zero rows",
            codelist_id=codelist_id,
        )

    by_key: dict[tuple[str, str | None, str, str], dict[str, Any]] = {}
    order: list[tuple[str, str | None, str, str]] = []
    unknown_statuses: list[str] = []
    seen_unknown: set[str] = set()

    for row in rows:
        code = _require_code(
            row, body=body, source_url=source_url, codelist_id=codelist_id
        )
        label = _require_label(
            row, code=code, body=body, source_url=source_url, codelist_id=codelist_id
        )

        # A property of this validity period, not of the code as a whole --
        # same reasoning as the area contract's status handling.
        status = _require_row_field(
            row,
            "status",
            code=code,
            body=body,
            source_url=source_url,
            codelist_id=codelist_id,
        ).lower()
        if status not in STATUS_DOMAIN and status not in seen_unknown:
            seen_unknown.add(status)
            unknown_statuses.append(status)

        crs = _require_row_field(
            row,
            "crs",
            code=code,
            body=body,
            source_url=source_url,
            codelist_id=codelist_id,
        )
        tossd = _require_row_field(
            row,
            "tossd",
            code=code,
            body=body,
            source_url=source_url,
            codelist_id=codelist_id,
        )

        activation_date = _parse_activation_date(
            row.get("activation-date"),
            code=code,
            body=body,
            source_url=source_url,
            codelist_id=codelist_id,
        )

        record = {
            "codelist_id": codelist_id,
            "code": code,
            "label": label,
            "status": status,
            "crs": crs,
            "tossd": tossd,
            "activation_date": activation_date,
            "description": _select_english_label(row.get("description")),
            "category": row.get("category"),
            # Load-bearing: 43 of 124 key-present Purpose-code rows carry
            # the empty string, not a real parent reference. `or None` turns
            # "" into None (-> pd.NA once the frame is built) without touching
            # a genuine value: every real code is a non-empty, truthy string,
            # so this can't mistake a real "0" for absence.
            "parent_code": row.get("parent-code") or None,
            "dac_reference": row.get("dac:reference"),
        }

        # crs/tossd distinguish the same code under different reporting
        # standards -- Purpose code 15190 and Channel of delivery 11000 are
        # worked examples.
        key = (code, activation_date, crs, tossd)
        existing = by_key.get(key)
        if existing is None:
            by_key[key] = record
            order.append(key)
        elif existing != record:
            raise CodelistShapeError(
                stage="row",
                detail=(
                    f"code {code!r} activation_date {activation_date!r} crs {crs!r} "
                    f"tossd {tossd!r} appears twice with conflicting values"
                ),
                body=body,
                url=source_url,
                codelist_id=codelist_id,
            )
        # else: a byte-identical duplicate key -- dropped silently, matching
        # the area contract's treatment.

    return [by_key[key] for key in order], unknown_statuses


def parse_code_categories(
    *,
    raw: Mapping[str, bytes],
    fetched_at: datetime,
    source_url: str = DEFAULT_URL,
) -> CodelistSnapshot:
    """Build a CodelistSnapshot from bytes already in hand, for the category contract.

    The category-contract counterpart of ``parse_codelists``: the same
    no-network escape hatch, over the 23 flat non-area codelists rather than
    Provider/Recipient. Every guarantee documented on ``CodelistSnapshot``
    holds here too -- ``content_hash``, ``raw`` replayability,
    ``unknown_statuses`` -- just computed over the eleven-column frame
    rather than the area contract's ten.

    Args:
        raw: ``codelist_id -> the exact response bytes`` for that codelist,
            in the order the caller wants ``codelist_ids`` to report. Each
            codelist_id should be one of ``SUPPORTED_CATEGORY_IDS``, though
            this function itself does not validate membership -- that is
            ``fetch_code_categories``'s job, before any network request is
            made; a caller building ``raw`` by hand (e.g. replaying a stored
            payload) is trusted to know what they are replaying.
        fetched_at: When *raw* was obtained. Must be timezone-aware, for the
            same reason as ``parse_codelists``.
        source_url: Recorded on the returned snapshot; does not affect
            parsing.

    Returns:
        A ``CodelistSnapshot`` whose ``frame`` holds one row per
        ``(codelist_id, code, activation_date, crs, tossd)`` across every
        codelist in *raw*. A code that means something different under CRS
        and TOSSD reporting carries one row per standard rather than one row
        overall.

    Raises:
        CodelistShapeError: A payload is not JSON, its envelope is
            malformed, or a row is missing a required field, carries a
            malformed ``activation_date``, or conflicts with another row
            sharing its five-column key.
        CodelistValidationError: *raw* is empty, a codelist in *raw* produced
            zero rows, or *fetched_at* is a naive datetime.
    """
    return _snapshot_from_raw(
        raw=raw,
        fetched_at=fetched_at,
        source_url=source_url,
        contract="category",
        columns=_CATEGORY_CONTRACT_COLUMNS,
        key_columns=_CATEGORY_KEY_COLUMNS,
        project_rows=_project_category_rows,
    )


def _validate_category_ids(codelist_ids: Sequence[str]) -> None:
    """Validate *codelist_ids* before ``fetch_code_categories`` issues any request.

    Mirrors ``_fetch.py``'s ``_validate_codelist_ids``: a bare ``str``,
    empty, a non-``str`` element, an id outside ``SUPPORTED_CATEGORY_IDS``,
    or a duplicate all raise ``CodelistValidationError`` naming the
    offending value, before the network is touched. Three ids get a more
    specific message than a bare "unsupported", since they are the
    wrong-ids a category-contract caller is most likely to try:
    ``"5"``/``"13"`` belong to the area contract, and ``"16"`` (Provider
    agency) belongs to the agency contract (``fetch_provider_agencies`` /
    ``parse_provider_agencies``).
    """
    # str IS a Sequence[str] -- codelist_ids="21" would otherwise iterate
    # into "2" and "1" and silently fetch two unrelated codelists instead of
    # raising. Checked before the empty check so codelist_ids="" reports the
    # type mistake, not "must not be empty".
    if isinstance(codelist_ids, str):
        raise CodelistValidationError(
            detail=(
                f"codelist_ids must be a sequence of ids, not a bare str: "
                f"{codelist_ids!r} is itself a Sequence[str], so it would be "
                f"iterated character-by-character rather than treated as one "
                f"id -- wrap it, e.g. ({codelist_ids!r},)"
            )
        )
    if not codelist_ids:
        raise CodelistValidationError(detail="codelist_ids must not be empty")
    seen: set[str] = set()
    for codelist_id in codelist_ids:
        if not isinstance(codelist_id, str):
            raise CodelistValidationError(
                detail=(
                    f"codelist_id {codelist_id!r} must be a str, not "
                    f"{type(codelist_id).__name__} -- pass it quoted, e.g. "
                    f"{str(codelist_id)!r}, not {codelist_id!r}"
                )
            )
        if codelist_id in ("5", "13"):
            area_name = "Provider" if codelist_id == "5" else "Recipient"
            raise CodelistValidationError(
                detail=(
                    f"codelist_id {codelist_id!r} ({area_name}) is an area codelist; "
                    "use fetch_codelists / parse_codelists instead of the category contract"
                ),
                codelist_id=codelist_id,
            )
        if codelist_id == "16":
            raise CodelistValidationError(
                detail=(
                    "codelist_id '16' (Provider agency) is an agency codelist; "
                    "an agency code is only meaningful within its donor, so "
                    "agency rows are keyed on (donor-code, code) rather than "
                    "the flat code a category codelist uses -- use "
                    "fetch_provider_agencies / parse_provider_agencies instead "
                    "of the category contract"
                ),
                codelist_id=codelist_id,
            )
        if codelist_id not in SUPPORTED_CATEGORY_IDS:
            raise CodelistValidationError(
                detail=(
                    f"unsupported codelist_id {codelist_id!r}; supported: "
                    f"{SUPPORTED_CATEGORY_IDS}"
                ),
                codelist_id=codelist_id,
            )
        if codelist_id in seen:
            raise CodelistValidationError(
                detail=f"duplicate codelist_id {codelist_id!r}",
                codelist_id=codelist_id,
            )
        seen.add(codelist_id)


def fetch_code_categories(
    *,
    codelist_ids: Sequence[str],
    timeout: int = 30,
    retries: int = 2,
) -> CodelistSnapshot:
    """Fetch and parse OECD DAC category codelists via the live ASPX handshake.

    The category-contract counterpart of ``fetch_codelists``, over the 23
    flat non-area codelists (everything except Provider, Recipient and
    Provider agency). ``parse_code_categories`` is composed
    with the same handshake ``fetch_codelists`` uses; every guarantee
    documented there holds here too.

    Unlike ``fetch_codelists``, *codelist_ids* has no default: there is no
    sensible default across 23 codelists, and defaulting to all of them
    would make a bulk fetch the accidental behaviour of a bare call.

    Args:
        codelist_ids: Which category codelists to fetch, in request order.
            Each must be one of ``SUPPORTED_CATEGORY_IDS``; duplicates are
            rejected. Required -- there is no default. The 23 supported ids:

            * ``"1"`` Bi_Multi
            * ``"2"`` Type of flow
            * ``"3"`` Channel of delivery
            * ``"4"`` Currency
            * ``"6"`` Nature of submission
            * ``"7"`` Markers
            * ``"8"`` Mobilisation leveraging
            * ``"9"`` Mobilisation origin
            * ``"10"`` Purpose code
            * ``"11"`` PSI additionality
            * ``"12"`` PSI flag
            * ``"14"`` Co-operation modality
            * ``"15"`` Type of finance
            * ``"17"`` Financing Arrangement
            * ``"18"`` Framework of collaboration
            * ``"19"`` TOSSD Pillar
            * ``"20"`` Agency Type
            * ``"21"`` Concessionality
            * ``"22"`` Keyword
            * ``"23"`` Markers value
            * ``"24"`` LDC Flag
            * ``"25"`` Type of Blended Finance
            * ``"26"`` Type of repayment

            Must be a sequence (e.g. a list), never a bare ``str``: since
            ``str`` is itself a ``Sequence[str]``, a bare id would be
            iterated character-by-character instead of treated as one id.
        timeout: Per-request timeout in seconds, applied to every request of
            every codelist's handshake.
        retries: Additional attempts after the first, per codelist, so total
            attempts per codelist is ``retries + 1``.

    Returns:
        A ``CodelistSnapshot`` covering every id in *codelist_ids*, with the
        eleven-column category frame.

    Raises:
        CodelistValidationError: *codelist_ids* is a bare ``str``, empty,
            contains a non-``str`` element, names an unsupported codelist
            (with a specific message for ``"5"``, ``"13"`` and ``"16"``), or
            contains a duplicate -- raised before any request is made. Also
            raised if a codelist's payload parses to zero rows.
        CodelistFetchError: Transport failures or 5xx/429/408/425 persisted
            through every attempt, for any one codelist.
        CodelistSourceError: A 4xx, an unfollowed redirect, or a 200
            response that is not the CodesList.aspx page, for any one
            codelist.
        CodelistShapeError: A required hidden token was absent or empty, or
            the downloaded payload's shape did not match what
            ``parse_code_categories`` expects.
    """
    _validate_category_ids(codelist_ids)
    raw: dict[str, bytes] = {
        codelist_id: _fetch_codelist_bytes(
            codelist_id, timeout=timeout, retries=retries
        )
        for codelist_id in codelist_ids
    }
    return parse_code_categories(
        raw=raw, fetched_at=datetime.now(UTC), source_url=DEFAULT_URL
    )
