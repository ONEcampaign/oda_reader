"""Envelope unwrap and frame build for OECD codelist JSON payloads.

``parse_codelists`` is the package's envelope reader and frame builder: the
envelope checks need to distinguish "not JSON" from "missing key" from
"wrong type" precisely, so ``_validate_envelope`` raises typed
``CodelistShapeError``s rather than leaving callers to catch incidental
``KeyError``/``ValueError``/``TypeError``.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Mapping
from datetime import date, datetime
from types import MappingProxyType
from typing import Any, Literal

import pandas as pd

from oda_reader.codelists._types import DEFAULT_URL, STATUS_DOMAIN, CodelistSnapshot
from oda_reader.exceptions import CodelistShapeError, CodelistValidationError

# The ten frame columns, in the documented order. content_hash's
# canonicalisation walks this exact order, so it is defined once here and
# reused by both the frame builder and the hash.
_CONTRACT_COLUMNS: tuple[str, ...] = (
    "codelist_id",
    "code",
    "label",
    "status",
    "type",
    "dotstat_code",
    "iso3",
    "crs",
    "tossd",
    "activation_date",
)

# pyarrow's date parser is lenient, so the source format is asserted
# before casting rather than trusted. The payload format is YYYY-MM-DD.
_ACTIVATION_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# content_hash canonicalisation: the sentinel for a null value and the
# separator joining a row's rendered fields. Neither can occur in a source
# value -- \x00 does not occur in OECD codes, and \x1f (unit separator) never
# appears in an OECD code, label, status or date.
_HASH_NULL_SENTINEL = "\x00"
_HASH_FIELD_SEP = "\x1f"

# Both spellings OECD uses for a narrative entry's language attribute. The
# unprefixed form appears on a top-level `name`; the underscore-prefixed one
# on the nested `Agencytype.name`. See _select_english_label.
_LANG_TAG_KEYS: tuple[str, ...] = ("xml:lang", "_xml:lang")


def _narrative_text(entry: Any) -> str | None:
    """The text of one ``narrative`` entry, whether it's a bare string or a ``{#text: ...}`` dict."""
    if isinstance(entry, str):
        return entry
    if isinstance(entry, dict):
        text = entry.get("#text")
        if isinstance(text, str):
            return text
    return None


def _language_tag(entry: Any) -> str | None:
    """The language-tag value on one ``narrative`` entry, under either spelling in ``_LANG_TAG_KEYS``."""
    if not isinstance(entry, dict):
        return None
    for key in _LANG_TAG_KEYS:
        value = entry.get(key)
        if isinstance(value, str):
            return value
    return None


def _is_english_tag(tag: str) -> bool:
    """True for an ``en`` language tag, case-insensitively, including a regional form like ``en-GB``."""
    tag = tag.lower()
    return tag == "en" or tag.startswith("en-")


def _select_english_label(name_field: Any) -> str | None:
    """Pick the English label out of a row's ``name.narrative`` field.

    Three-step priority, in order:

    1. An entry explicitly tagged ``en`` (either spelling in
       ``_LANG_TAG_KEYS``, case-insensitively, regional forms like ``en-GB``
       included) wins outright, wherever it sits in the array.
    2. Otherwise, the shape OECD actually publishes today: no row's
       ``name.narrative`` tags English at all, only the French variant, so
       the first untagged entry is taken to be English.
    3. Otherwise, ``narrative[0]``, so a malformed or all-tagged array still
       returns something rather than ``None``.

    Step 1 exists because step 2 is a heuristic, not a guarantee: an
    untagged entry is *probably* English only because no row observed so far
    tags it explicitly. The moment OECD starts tagging English too, treating
    "untagged" as "English" would pick whichever language happens to sit
    first instead. The *language* returned is the semver promise; this
    selector is an implementation detail we may change in a patch release if
    OECD reorders the array or changes its tagging further.

    Both spellings in ``_LANG_TAG_KEYS`` count as a language tag at every
    step. Top-level ``name`` uses ``xml:lang``, but the nested
    ``Agencytype.name`` the agency contract projects uses ``_xml:lang`` with
    a leading underscore -- on all 1,374 agency rows. Recognising only the
    unprefixed form would leave step 2's guard doing nothing there, picking
    English purely because it happens to sit first in the array.
    """
    if not isinstance(name_field, dict):
        return None
    narrative = name_field.get("narrative")
    if not isinstance(narrative, list) or not narrative:
        return None

    untagged_text: str | None = None
    for entry in narrative:
        tag = _language_tag(entry)
        if tag is not None and _is_english_tag(tag):
            text = _narrative_text(entry)
            if text is not None:
                return text
        elif tag is None and untagged_text is None:
            untagged_text = _narrative_text(entry)
    if untagged_text is not None:
        return untagged_text
    return _narrative_text(narrative[0])


def _validate_envelope(
    *, codelist_id: str, body: bytes, source_url: str
) -> list[dict[str, Any]]:
    """Parse *body* as the OECD JSON envelope and return its item rows.

    A payload that is not JSON, or whose ``codelists.codelist`` key is
    missing / not a list / empty / carries more than one block, or whose
    ``codelist-items.codelist-item`` key is missing / not a list, is not our
    document -- each raises ``CodelistShapeError(stage="envelope")`` with
    *body* attached.

    The fetch handshake validates only that the *page* stage returned
    ``CodesList.aspx``, deliberately leaving download-stage envelope
    validation to this one place so the two checks can't drift apart.
    """
    try:
        payload = json.loads(body)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise CodelistShapeError(
            stage="envelope",
            detail=f"response body is not valid JSON: {exc}",
            body=body,
            url=source_url,
            codelist_id=codelist_id,
        ) from exc

    codelists_node = payload.get("codelists") if isinstance(payload, dict) else None
    codelist_list = (
        codelists_node.get("codelist") if isinstance(codelists_node, dict) else None
    )
    if not isinstance(codelist_list, list):
        raise CodelistShapeError(
            stage="envelope",
            detail="payload['codelists']['codelist'] is missing or not a list",
            body=body,
            url=source_url,
            codelist_id=codelist_id,
        )
    if not codelist_list:
        raise CodelistShapeError(
            stage="envelope",
            detail="payload['codelists']['codelist'] is empty",
            body=body,
            url=source_url,
            codelist_id=codelist_id,
        )
    if len(codelist_list) > 1:
        raise CodelistShapeError(
            stage="envelope",
            detail=(
                f"payload['codelists']['codelist'] carries {len(codelist_list)} "
                f"blocks, not 1, for requested codelist_id {codelist_id!r}; one "
                "request returns one codelist, so there is no single correct "
                "codelist_id to label every block with. This shape usually means "
                'the bytes came from an "All codes list" (id=0) request rather '
                "than a request for this specific codelist -- split it into one "
                "single-block payload per codelist_id before calling."
            ),
            body=body,
            url=source_url,
            codelist_id=codelist_id,
        )

    codelist_node = codelist_list[0]
    items_node = (
        codelist_node.get("codelist-items") if isinstance(codelist_node, dict) else None
    )
    rows = items_node.get("codelist-item") if isinstance(items_node, dict) else None
    if not isinstance(rows, list):
        raise CodelistShapeError(
            stage="envelope",
            detail="payload[...]['codelist-items']['codelist-item'] is missing",
            body=body,
            url=source_url,
            codelist_id=codelist_id,
        )
    return rows


def _parse_activation_date(
    value: Any, *, code: str, body: bytes, source_url: str, codelist_id: str
) -> str | None:
    """Validate ``activation-date`` and return it unchanged, or ``None`` if absent.

    Shared by the area contract's ``_project_rows`` and the category
    contract's row projector -- both codelist families use the same source
    field and the same validation. pyarrow's date parser is lenient, so the
    source format is asserted before casting rather than trusted: the regex
    only asserts digit-shape (``"2026-99-99"`` matches it), so a real
    calendar date is checked separately. A non-string value (the payload is
    JSON -- it could be a number, list or dict) fails ``isinstance()``
    before ``.match()`` ever sees it, so neither escape reaches the caller
    as an untyped exception.
    """
    if value is None:
        return None
    if not isinstance(value, str) or not _ACTIVATION_DATE_RE.match(value):
        raise CodelistShapeError(
            stage="row",
            detail=f"code {code!r} has activation_date {value!r}, expected YYYY-MM-DD",
            body=body,
            url=source_url,
            codelist_id=codelist_id,
        )
    try:
        date.fromisoformat(value)
    except ValueError as exc:
        raise CodelistShapeError(
            stage="row",
            detail=(
                f"code {code!r} has activation_date {value!r}, "
                f"not a real calendar date: {exc}"
            ),
            body=body,
            url=source_url,
            codelist_id=codelist_id,
        ) from exc
    return value


def _require_code(
    row: dict[str, Any], *, body: bytes, source_url: str, codelist_id: str
) -> str:
    """A row's non-empty ``code`` string, or ``CodelistShapeError(stage="row")``.

    Shared by the area contract's ``_project_rows`` and the category
    contract's ``_project_category_rows`` -- both project the same source
    field, checked the same way.
    """
    code = row.get("code")
    if not isinstance(code, str) or not code:
        raise CodelistShapeError(
            stage="row",
            detail="a row has no 'code' field",
            body=body,
            url=source_url,
            codelist_id=codelist_id,
        )
    return code


def _require_label(
    row: dict[str, Any], *, code: str, body: bytes, source_url: str, codelist_id: str
) -> str:
    """The row's derivable English label, or ``CodelistShapeError(stage="row")``.

    Shared by the area contract's ``_project_rows`` and the category
    contract's ``_project_category_rows``.
    """
    label = _select_english_label(row.get("name"))
    if label is None:
        raise CodelistShapeError(
            stage="row",
            detail=f"code {code!r} has no derivable English label",
            body=body,
            url=source_url,
            codelist_id=codelist_id,
        )
    return label


def _require_row_field(
    row: dict[str, Any],
    field: str,
    *,
    code: str,
    body: bytes,
    source_url: str,
    codelist_id: str,
) -> str:
    """A non-empty string value for *field*, or ``CodelistShapeError(stage="row")``.

    Shared by the area contract's ``status`` check and the category
    contract's ``status``/``crs``/``tossd`` checks -- every one of them is
    "a required, never-null string column" with the same failure shape.
    """
    value = row.get(field)
    if not isinstance(value, str) or not value:
        raise CodelistShapeError(
            stage="row",
            detail=f"code {code!r} has no {field!r} field",
            body=body,
            url=source_url,
            codelist_id=codelist_id,
        )
    return value


def _project_rows(
    *, codelist_id: str, rows: list[dict[str, Any]], body: bytes, source_url: str
) -> tuple[list[dict[str, Any]], list[str]]:
    """Project *rows* onto the ten contract columns for one codelist.

    Returns the projected records (one per unique ``(code, activation_date)``
    pair) and the distinct out-of-domain ``status`` values seen, in
    first-seen order.

    The grain is ``(codelist_id, code, activation_date)``. OECD publishes
    codelist 13 as validity periods: a code can carry several rows, one per
    period of its history, differing in ``activation_date`` and ``status``.
    Those are one entity's history and all of them stay in the frame.
    ``pd.NA`` (no ``activation_date``) is a legitimate third component of
    the key, not a reason to collapse rows -- every codelist-5 row keys
    this way.

    A row missing ``code``, a derivable label, or ``status``, or carrying a
    malformed ``activation_date``, raises ``CodelistShapeError(stage="row")``.
    Two rows sharing a full three-column
    key with conflicting projected values also raise; two rows sharing the
    key with identical projected values are silently deduplicated. An empty
    *rows* raises ``CodelistValidationError`` -- OECD returning zero items
    for a requested codelist is a self-consistency failure, not a shape
    one, so it gets the validation exception rather than the shape one.
    """
    if not rows:
        raise CodelistValidationError(
            detail=f"codelist {codelist_id!r} returned zero rows",
            codelist_id=codelist_id,
        )

    by_key: dict[tuple[str, str | None], dict[str, Any]] = {}
    order: list[tuple[str, str | None]] = []
    unknown_statuses: list[str] = []
    seen_unknown: set[str] = set()

    for row in rows:
        code = _require_code(
            row, body=body, source_url=source_url, codelist_id=codelist_id
        )
        label = _require_label(
            row, code=code, body=body, source_url=source_url, codelist_id=codelist_id
        )

        # A property of this validity period, not of the code as a whole.
        # A code with several periods can be "withdrawn" in an earlier one
        # and "active" in its most recent.
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
            "type": row.get("type"),
            "dotstat_code": row.get("dotstatcode"),
            "iso3": row.get("iso-alpha-3-code"),
            "crs": row.get("crs"),
            "tossd": row.get("tossd"),
            "activation_date": activation_date,
        }

        # `activation_date` (a plain str or None here, not yet pd.NA) is
        # part of the key: `None == None` compares correctly in a Python
        # dict, so this is NA-safe by construction -- no pandas groupby or
        # merge is involved in this dedup, so there's no NA-drops-the-row
        # footgun to guard against here.
        key = (code, activation_date)
        existing = by_key.get(key)
        if existing is None:
            by_key[key] = record
            order.append(key)
        elif existing != record:
            raise CodelistShapeError(
                stage="row",
                detail=(
                    f"code {code!r} activation_date {activation_date!r} "
                    "appears twice with conflicting values"
                ),
                body=body,
                url=source_url,
                codelist_id=codelist_id,
            )
        # else: a byte-identical duplicate key -- dropped silently.

    return [by_key[key] for key in order], unknown_statuses


def _build_frame(
    records: list[dict[str, Any]], columns: tuple[str, ...]
) -> pd.DataFrame:
    """Assemble a contract frame from projected row records.

    Shared by the area contract's ten columns and the category contract's
    eleven -- ``columns`` is the only thing that differs between them.
    ``string[pyarrow]`` on every column except ``activation_date``, which is
    ``date32[pyarrow]``. A missing value in a record becomes ``pd.NA`` under
    either dtype, never an empty string.
    """
    frame = pd.DataFrame.from_records(records, columns=list(columns))
    for column in columns:
        if column == "activation_date":
            frame[column] = frame[column].astype("date32[pyarrow]")
        else:
            frame[column] = frame[column].astype("string[pyarrow]")
    return frame


def _render_hash_value(column: str, value: Any) -> str:
    """Render one cell: strings as themselves, ``activation_date`` as
    ISO-8601, ``pd.NA`` as the ``\\x00`` sentinel."""
    if pd.isna(value):
        return _HASH_NULL_SENTINEL
    if column == "activation_date":
        return value.isoformat()
    return str(value)


def _compute_content_hash(
    frame: pd.DataFrame, *, columns: tuple[str, ...], key_columns: tuple[str, ...]
) -> str:
    """Compute ``content_hash`` per the six-step canonicalisation below.

    Shared by both contracts: the area contract calls this with its ten
    columns and its three-column key; the category contract calls it with
    its eleven columns and its five-column key. Only *columns*
    and *key_columns* change between them -- the recipe itself does not.

    1. *columns*, in the documented order.
    2. Rows sorted by *key_columns*, comparing UTF-8 bytes; a ``pd.NA`` key
       value sorts first because it renders as the ``\\x00`` sentinel of
       step 3.
    3. Each value rendered: strings as themselves, ``activation_date`` as
       ISO-8601 ``YYYY-MM-DD``, ``pd.NA`` as the single byte ``\\x00``.
    4. A row's rendered values joined with ``\\x1f``.
    5. Rows joined with ``\\n``.
    6. UTF-8 encoded, sha256'd, hex-digested, prefixed ``"v1:"``.

    Hashes do not compare across prefix versions.
    """
    key_indices = [columns.index(column) for column in key_columns]
    rendered_rows = [
        tuple(
            _render_hash_value(column, value)
            for column, value in zip(columns, row, strict=True)
        )
        for row in frame[list(columns)].itertuples(index=False, name=None)
    ]
    ordered = sorted(
        rendered_rows,
        key=lambda row: tuple(row[index].encode("utf-8") for index in key_indices),
    )

    lines = [_HASH_FIELD_SEP.join(row) for row in ordered]
    canonical = "\n".join(lines)
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return f"v1:{digest}"


def _snapshot_from_raw(
    *,
    raw: Mapping[str, bytes],
    fetched_at: datetime,
    source_url: str,
    contract: Literal["area", "category", "agency"],
    columns: tuple[str, ...],
    key_columns: tuple[str, ...],
    project_rows: Callable[..., tuple[list[dict[str, Any]], list[str]]],
) -> CodelistSnapshot:
    """Shared machinery behind ``parse_codelists`` and ``parse_code_categories``.

    Both public functions validate ``fetched_at``, walk *raw* through
    ``_validate_envelope`` and their own *project_rows*, accumulate
    ``unknown_statuses`` in first-seen order across every codelist, then
    build the frame and ``content_hash``. Only *contract*, *columns*,
    *key_columns* and *project_rows* differ between the area and category
    contracts -- this function is not part of the public surface, so each
    public function keeps its own full docstring describing its contract.
    """
    if fetched_at.tzinfo is None or fetched_at.tzinfo.utcoffset(fetched_at) is None:
        raise CodelistValidationError(
            detail=(
                f"fetched_at must be timezone-aware, got a naive datetime "
                f"{fetched_at!r}; use datetime.datetime.now(datetime.UTC)"
            ),
        )
    if not raw:
        raise CodelistValidationError(
            detail=(
                "raw must not be empty: an empty mapping produces a "
                "valid-looking snapshot with zero rows instead of raising, "
                "the same failure a single codelist returning zero rows is "
                "already rejected for -- and since retirement is scoped to "
                "the codelists a snapshot covers, feeding an empty one to "
                "reconcile reports is_unchanged on a run that read nothing "
                "at all. If raw came from a stored payload, "
                "check that the file was actually read (a missing or empty "
                "file silently 'succeeding' is the common cause); if it came "
                "from fetch_*, check that codelist_ids was non-empty."
            ),
        )

    codelist_ids = tuple(raw)
    records: list[dict[str, Any]] = []
    unknown_statuses: list[str] = []
    seen_unknown: set[str] = set()

    for codelist_id, body in raw.items():
        rows = _validate_envelope(
            codelist_id=codelist_id, body=body, source_url=source_url
        )
        projected, codelist_unknown = project_rows(
            codelist_id=codelist_id, rows=rows, body=body, source_url=source_url
        )
        records.extend(projected)
        for status in codelist_unknown:
            if status not in seen_unknown:
                seen_unknown.add(status)
                unknown_statuses.append(status)

    frame = _build_frame(records, columns)
    content_hash = _compute_content_hash(
        frame, columns=columns, key_columns=key_columns
    )

    return CodelistSnapshot(
        contract=contract,
        frame=frame,
        raw=MappingProxyType(dict(raw)),
        fetched_at=fetched_at,
        source_url=source_url,
        codelist_ids=codelist_ids,
        content_hash=content_hash,
        unknown_statuses=tuple(unknown_statuses),
    )


def parse_codelists(
    *,
    raw: Mapping[str, bytes],
    fetched_at: datetime,
    source_url: str = DEFAULT_URL,
) -> CodelistSnapshot:
    """Build a CodelistSnapshot from bytes already in hand, with no network access.

    The consumer's escape hatch when the live OECD page is broken: every
    guarantee documented on ``CodelistSnapshot`` holds equally on a payload
    obtained any other way -- yesterday's snapshot, a colleague's manual
    download.

    Args:
        raw: ``codelist_id -> the exact response bytes`` for that codelist,
            in the order the caller wants ``codelist_ids`` to report.
        fetched_at: When *raw* was obtained. Must be timezone-aware --
            ``CodelistSnapshot.fetched_at`` is documented as such, and
            ``reconcile`` later calls ``.astimezone(UTC)`` on it, which
            would silently reinterpret a naive value in the machine's local
            timezone. Rejected rather than assumed-UTC: a wrong-by-hours
            timestamp written into lineage columns is worse than a loud
            failure here. Use ``datetime.datetime.now(datetime.UTC)``.
        source_url: Recorded on the returned snapshot; does not affect
            parsing.

    Returns:
        A ``CodelistSnapshot`` whose ``frame`` holds one row per
        ``(codelist_id, code, activation_date)`` across every codelist in
        *raw*. OECD publishes codelist 13 as validity periods, so a code
        with a withdrawn-then-reactivated history carries one row per
        period rather than one row overall. ``status`` is therefore a
        property of a period, not of a code -- selecting "is this code
        currently active" means picking the row with the greatest
        ``activation_date`` for that code.

    Raises:
        CodelistShapeError: A payload is not JSON, its envelope is
            malformed, or a row is missing a required field, carries a
            malformed ``activation_date``, or conflicts with another row
            sharing its ``(code, activation_date)`` key.
        CodelistValidationError: *raw* is empty, a codelist in *raw* produced
            zero rows, or *fetched_at* is a naive datetime.
    """
    return _snapshot_from_raw(
        raw=raw,
        fetched_at=fetched_at,
        source_url=source_url,
        contract="area",
        columns=_CONTRACT_COLUMNS,
        key_columns=("codelist_id", "code", "activation_date"),
        project_rows=_project_rows,
    )
