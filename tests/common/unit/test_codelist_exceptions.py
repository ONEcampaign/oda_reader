"""Unit tests for the oda_reader.codelists exception hierarchy."""

import pytest

import oda_reader.exceptions as mod
from oda_reader.exceptions import (
    CodelistError,
    CodelistFetchError,
    CodelistShapeError,
    CodelistSourceError,
    CodelistValidationError,
)

_ALL_CODELIST_EXCEPTIONS = (
    CodelistError,
    CodelistFetchError,
    CodelistSourceError,
    CodelistShapeError,
    CodelistValidationError,
)

_VIEWSTATE_HTML = (
    b"<html><body><form>"
    b'<input type="hidden" name="__VIEWSTATE" id="__VIEWSTATE" '
    b'value="SECRET_TOKEN_MARKER_DO_NOT_LEAK" />'
    b'<input type="hidden" name="__EVENTVALIDATION" '
    b"value='ANOTHER_SECRET_MARKER' />"
    b'<input type="text" name="tb_search" value="" />'
    b"</form></body></html>"
)


@pytest.mark.unit
def test_codelist_error_base_constructs() -> None:
    """CodelistError constructs with its documented kwargs."""
    exc = CodelistError(message="something broke", codelist_id="5")
    assert exc.codelist_id == "5"
    assert str(exc) == "something broke"


@pytest.mark.unit
def test_codelist_fetch_error_constructs() -> None:
    """CodelistFetchError constructs with its documented kwargs."""
    exc = CodelistFetchError(
        url="https://example.org/CodesList.aspx",
        stage="page",
        attempts=3,
        status_code=503,
        codelist_id="13",
    )
    assert exc.url == "https://example.org/CodesList.aspx"
    assert exc.stage == "page"
    assert exc.attempts == 3
    assert exc.status_code == 503
    assert exc.codelist_id == "13"


@pytest.mark.unit
def test_codelist_source_error_constructs() -> None:
    """CodelistSourceError constructs with its documented kwargs."""
    exc = CodelistSourceError(
        url="https://example.org/CodesList.aspx",
        stage="select",
        status_code=404,
        final_url="https://example.org/moved",
        location="https://example.org/moved",
        body=b"not found",
        codelist_id="5",
    )
    assert exc.url == "https://example.org/CodesList.aspx"
    assert exc.stage == "select"
    assert exc.status_code == 404
    assert exc.final_url == "https://example.org/moved"
    assert exc.location == "https://example.org/moved"
    assert exc.body == b"not found"
    assert exc.codelist_id == "5"


@pytest.mark.unit
def test_codelist_shape_error_constructs() -> None:
    """CodelistShapeError constructs with its documented kwargs, including url."""
    exc = CodelistShapeError(
        stage="hidden-field",
        detail="__VIEWSTATE missing",
        body=b"<html></html>",
        url="https://example.org/CodesList.aspx",
        codelist_id="5",
    )
    assert exc.stage == "hidden-field"
    assert exc.detail == "__VIEWSTATE missing"
    assert exc.body == b"<html></html>"
    assert exc.url == "https://example.org/CodesList.aspx"
    assert exc.codelist_id == "5"


@pytest.mark.unit
def test_codelist_validation_error_constructs() -> None:
    """CodelistValidationError constructs with its documented kwargs."""
    exc = CodelistValidationError(detail="zero rows returned", codelist_id="13")
    assert exc.detail == "zero rows returned"
    assert exc.codelist_id == "13"


@pytest.mark.unit
def test_is_retryable_true_only_on_fetch_error() -> None:
    """is_retryable is True on exactly CodelistFetchError, False on the other four."""
    assert CodelistFetchError(url="u", stage="page", attempts=1).is_retryable is True
    assert CodelistError(message="m").is_retryable is False
    assert CodelistSourceError(url="u", stage="page").is_retryable is False
    assert CodelistShapeError(stage="row", detail="d").is_retryable is False
    assert CodelistValidationError(detail="d").is_retryable is False


@pytest.mark.unit
@pytest.mark.parametrize("exc_type", _ALL_CODELIST_EXCEPTIONS)
def test_every_class_is_catchable_as_codelist_error(exc_type) -> None:
    """Every codelist exception is catchable as the common CodelistError base."""
    assert issubclass(exc_type, CodelistError)


@pytest.mark.unit
def test_codelist_fetch_error_is_not_an_os_error() -> None:
    """CodelistFetchError must not subclass ConnectionError/OSError, or a
    consumer's `except OSError` around a fetch plus a file write would swallow
    a codelist failure as a disk problem.
    """
    exc = CodelistFetchError(url="u", stage="page", attempts=3)
    assert not isinstance(exc, OSError)
    assert not isinstance(exc, ConnectionError)


@pytest.mark.unit
def test_body_over_64kib_is_truncated_to_exactly_65536_bytes() -> None:
    """A body longer than 64 KiB is truncated to exactly 65536 bytes."""
    oversized = b"x" * (128 * 1024)
    exc = CodelistSourceError(url="u", stage="page", body=oversized)
    assert exc.body is not None
    assert len(exc.body) == 64 * 1024


@pytest.mark.unit
def test_body_none_stays_none() -> None:
    """body=None stays None on both exceptions that accept it."""
    source_exc = CodelistSourceError(url="u", stage="page", body=None)
    shape_exc = CodelistShapeError(stage="row", detail="d", body=None)
    assert source_exc.body is None
    assert shape_exc.body is None


@pytest.mark.unit
def test_str_never_contains_body_marker() -> None:
    """str(exc) never reproduces a marker present in body. Composing __str__
    from the raw body would let a bare `print(exc)` or traceback dump 64 KiB
    of page HTML into logs or CI output.
    """
    body = b"<html>" + b"UNIQUE_BODY_MARKER_XYZ" * 100 + b"</html>"
    source_exc = CodelistSourceError(
        url="https://example.org", stage="page", status_code=403, body=body
    )
    shape_exc = CodelistShapeError(stage="envelope", detail="missing key", body=body)
    assert "UNIQUE_BODY_MARKER_XYZ" not in str(source_exc)
    assert "UNIQUE_BODY_MARKER_XYZ" not in str(shape_exc)
    # sanity: the marker really is on the exception, just not in __str__
    assert source_exc.body is not None and b"UNIQUE_BODY_MARKER_XYZ" in source_exc.body


@pytest.mark.unit
def test_hidden_token_values_are_redacted_from_body() -> None:
    """Hidden ASP.NET token values are redacted before body is stored.
    A stage="hidden-field" failure must not carry a live __VIEWSTATE (or
    similar) into whatever log aggregator the consumer runs.
    """
    exc = CodelistShapeError(
        stage="hidden-field", detail="__VIEWSTATE empty", body=_VIEWSTATE_HTML
    )
    assert exc.body is not None
    assert b"SECRET_TOKEN_MARKER_DO_NOT_LEAK" not in exc.body
    assert b"ANOTHER_SECRET_MARKER" not in exc.body
    # the redaction is targeted: non-token markup and other attributes survive
    assert b"__VIEWSTATE" in exc.body
    assert b"__EVENTVALIDATION" in exc.body
    assert b'name="tb_search"' in exc.body


@pytest.mark.unit
def test_redaction_matches_uppercase_and_mixed_case_name_attribute() -> None:
    """`name` matching must be case-insensitive. `<input NAME="__VIEWSTATE"
    value="...">` is valid HTML; the regex previously had no IGNORECASE flag,
    so the tag was never recognised and the whole tag -- value included --
    passed through unredacted."""
    body = (
        b'<input type="hidden" NAME="__VIEWSTATE" '
        b'value="SECRET_UPPERCASE_NAME_MARKER" />'
    )
    redacted = mod._redact_hidden_token_values(body)
    assert b"SECRET_UPPERCASE_NAME_MARKER" not in redacted

    body_mixed = (
        b'<input type="hidden" Name="__VIEWSTATE" '
        b'value="SECRET_MIXEDCASE_NAME_MARKER" />'
    )
    redacted_mixed = mod._redact_hidden_token_values(body_mixed)
    assert b"SECRET_MIXEDCASE_NAME_MARKER" not in redacted_mixed


@pytest.mark.unit
def test_redaction_handles_unquoted_value() -> None:
    """`<input name=__VIEWSTATE value=SECRET>` is valid HTML. The value regex
    previously required quotes around the value, so an unquoted value was
    never substituted."""
    body = b"<input type=hidden name=__VIEWSTATE value=SECRET_UNQUOTED_MARKER>"
    redacted = mod._redact_hidden_token_values(body)
    assert b"SECRET_UNQUOTED_MARKER" not in redacted


@pytest.mark.unit
def test_redaction_handles_single_quoted_value() -> None:
    """`<input name='__VIEWSTATE' value='SECRET'>` must be redacted too."""
    body = b"<input type='hidden' name='__VIEWSTATE' value='SECRET_SINGLEQUOTE_MARKER'>"
    redacted = mod._redact_hidden_token_values(body)
    assert b"SECRET_SINGLEQUOTE_MARKER" not in redacted


@pytest.mark.unit
def test_redaction_withholds_tag_with_no_value_attribute() -> None:
    """A hidden `__`-prefixed input with no `value` attribute at all cannot
    be value-substituted. The fail-closed guarantee requires withholding the
    whole tag rather than emitting it -- matching the parse-failure idiom
    that already returns a placeholder instead of the raw body."""
    body = b'before<input type="hidden" name="__VIEWSTATE" />after'
    redacted = mod._redact_hidden_token_values(body)
    assert b'<input type="hidden" name="__VIEWSTATE" />' not in redacted
    # Surrounding text is untouched -- only the tag itself is withheld.
    assert b"before" in redacted
    assert b"after" in redacted


@pytest.mark.unit
def test_redaction_not_shadowed_by_preceding_data_value_attribute() -> None:
    """`data-value="..."` ends in the literal substring `value=`. A regex
    with no left boundary matches inside it, consuming the one-and-only
    substitution before the real `value=` attribute is ever reached --
    leaking the real value untouched. The real attribute must still be
    redacted regardless of what other `*value=`-suffixed attributes
    precede it."""
    body = (
        b'<input type="hidden" name="__VIEWSTATE" data-value="harmless" '
        b'value="SECRET_SHADOWED_BY_DATA_VALUE" />'
    )
    redacted = mod._redact_hidden_token_values(body)
    assert b"SECRET_SHADOWED_BY_DATA_VALUE" not in redacted


@pytest.mark.unit
def test_redaction_not_shadowed_by_following_data_value_attribute() -> None:
    """Same hazard, decoy attribute after the real one."""
    body = (
        b'<input type="hidden" name="__VIEWSTATE" '
        b'value="SECRET_BEFORE_DATA_VALUE" data-value="harmless" />'
    )
    redacted = mod._redact_hidden_token_values(body)
    assert b"SECRET_BEFORE_DATA_VALUE" not in redacted


@pytest.mark.unit
def test_redaction_not_shadowed_by_aria_valuenow_attribute() -> None:
    """`aria-valuenow="3"` is another `*value*=`-shaped decoy attribute
    name; the real `value=` must still be found and redacted."""
    body = (
        b'<input type="hidden" name="__VIEWSTATE" aria-valuenow="3" '
        b'value="SECRET_AFTER_ARIA_VALUENOW" />'
    )
    redacted = mod._redact_hidden_token_values(body)
    assert b"SECRET_AFTER_ARIA_VALUENOW" not in redacted


@pytest.mark.unit
def test_redaction_handles_two_value_attributes() -> None:
    """A tag with two `value=` attributes (malformed but parseable HTML)
    must have both redacted -- substituting only the first is the wrong
    direction for a fail-closed helper."""
    body = (
        b'<input type="hidden" name="__VIEWSTATE" value="SECRET_FIRST_VALUE" '
        b'value="SECRET_SECOND_VALUE" />'
    )
    redacted = mod._redact_hidden_token_values(body)
    assert b"SECRET_FIRST_VALUE" not in redacted
    assert b"SECRET_SECOND_VALUE" not in redacted


@pytest.mark.unit
def test_redaction_failure_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    """A parse exception during redaction withholds the body instead of leaking it raw.

    Returning the original bytes on a parse failure would be the one path
    that lets a live __VIEWSTATE reach a log aggregator or CI transcript --
    exactly when malformed input triggers an exception body in the first
    place. Redaction must fail closed, not open.
    """

    def _raise(self: object, text: str) -> None:
        raise ValueError("forced parse failure")

    monkeypatch.setattr(mod._HiddenTokenFinder, "feed", _raise)

    exc = CodelistShapeError(
        stage="hidden-field", detail="__VIEWSTATE empty", body=_VIEWSTATE_HTML
    )
    assert exc.body is not None
    assert b"SECRET_TOKEN_MARKER_DO_NOT_LEAK" not in exc.body
    assert b"ANOTHER_SECRET_MARKER" not in exc.body
    assert exc.body != _VIEWSTATE_HTML


@pytest.mark.unit
@pytest.mark.parametrize(
    "call",
    [
        lambda: CodelistError("positional message"),  # type: ignore[call-arg]
        lambda: CodelistFetchError("u", "page", 1),  # type: ignore[call-arg]
        lambda: CodelistSourceError("u", "page"),  # type: ignore[call-arg]
        lambda: CodelistShapeError("row", "detail"),  # type: ignore[call-arg]
        lambda: CodelistValidationError("detail"),  # type: ignore[call-arg]
    ],
)
def test_constructors_are_keyword_only(call) -> None:
    """Every codelist exception constructor is keyword-only (§0.3)."""
    with pytest.raises(TypeError):
        call()
