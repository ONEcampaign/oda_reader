"""Unit tests for the DAC2 -> DAC2-generic rename's deprecated aliases.

These pin the layer-1 refactor's defining property: every existing DAC2A
public call keeps working with identical results.
"""

import pytest

from oda_reader import QueryBuilder, get_available_filters
from oda_reader.schemas.dac2_translation import convert_dac2a_to_dotstat_codes
from oda_reader.schemas.schema_tools import read_schema_translation


@pytest.mark.unit
class TestBuildDac2aFilterAlias:
    """`build_dac2a_filter` is a deprecated alias for `build_dac2_filter`."""

    def test_warns_and_matches_new_name(self):
        """The old name warns and returns the same string as the new one."""
        qb = QueryBuilder(dataflow_id="DSD_DAC2@DF_DAC2A")
        kwargs = {"donor": "USA", "recipient": "KEN", "measure": 2101}

        with pytest.warns(DeprecationWarning, match="build_dac2_filter"):
            old_result = qb.build_dac2a_filter(**kwargs)

        new_result = qb.build_dac2_filter(**kwargs)

        assert old_result == new_result

    def test_annotations_match_via_functools_wraps(self):
        """`functools.wraps` must carry `__annotations__` onto the alias.

        `get_available_filters` reads `__annotations__` off whichever object
        it is handed; a bare wrapper would return an empty filter dict for
        anyone still on the old name.
        """
        assert (
            QueryBuilder.build_dac2a_filter.__annotations__
            == QueryBuilder.build_dac2_filter.__annotations__
        )

    def test_alias_keeps_its_own_identity(self):
        """`functools.wraps` must not erase the alias's own name/docstring.

        `functools.wraps` also copies `__name__`/`__qualname__`/`__doc__`
        from the wrapped function; those must be restored to the alias's
        own so `help()` and tracebacks still show it as deprecated.
        """
        assert QueryBuilder.build_dac2a_filter.__name__ == "build_dac2a_filter"
        assert "deprecated" in QueryBuilder.build_dac2a_filter.__doc__.lower()

    def test_get_available_filters_dac2a_unchanged(self):
        """`get_available_filters("dac2a")` is unchanged and non-empty."""
        filters = get_available_filters("dac2a", quiet=True)

        assert filters
        assert "donor" in filters
        assert "recipient" in filters


@pytest.mark.unit
class TestConvertDac2aToDotstatCodesAlias:
    """`convert_dac2a_to_dotstat_codes` is a deprecated alias."""

    def test_warns_and_delegates(self, mocker):
        """The old name warns and delegates to the DAC2-generic function."""
        mock_convert = mocker.patch(
            "oda_reader.schemas.dac2_translation.convert_dac2_to_dotstat_codes",
        )
        df = mocker.sentinel.df

        with pytest.warns(DeprecationWarning, match="convert_dac2_to_dotstat_codes"):
            result = convert_dac2a_to_dotstat_codes(df)

        mock_convert.assert_called_once_with(df)
        assert result is mock_convert.return_value


@pytest.mark.unit
class TestReadSchemaTranslationDac2aAlias:
    """`read_schema_translation("dac2a")` resolves through the DAC2 alias."""

    def test_returns_same_mapping_as_before_the_rename(self):
        """Known keys survive the `dac2a_dotstat.json` -> `dac2_dotstat.json` rename."""
        mapping = read_schema_translation("dac2a")

        assert mapping["DONOR"]["name"] == "donor_code"
        assert mapping["MEASURE"]["name"] == "aidtype_code"
