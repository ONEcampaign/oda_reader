"""Test utilities and helper functions."""

import json
from pathlib import Path
from typing import Any

import pandas as pd


def assert_dataframe_schema(df: pd.DataFrame, expected_columns: dict[str, str]) -> None:
    """Validate DataFrame has expected columns and types.

    Args:
        df: DataFrame to validate
        expected_columns: Dict mapping column names to expected dtypes

    Raises:
        AssertionError: If schema doesn't match expectations
    """
    for col, dtype in expected_columns.items():
        assert col in df.columns, f"Column '{col}' not found in DataFrame"
        assert (
            str(df[col].dtype) == dtype
        ), f"Column '{col}' has dtype {df[col].dtype}, expected {dtype}"


def load_json_fixture(fixtures_dir: Path, fixture_name: str) -> Any:
    """Load a JSON fixture file.

    Args:
        fixtures_dir: Path to fixtures directory
        fixture_name: Name of the fixture file (without .json extension)

    Returns:
        Parsed JSON data
    """
    fixture_path = fixtures_dir / "api_responses" / f"{fixture_name}.json"
    with open(fixture_path) as f:
        return json.load(f)


def mock_api_response(status_code: int, text: str, from_cache: bool = False) -> tuple:
    """Create a mock API response tuple.

    Args:
        status_code: HTTP status code
        text: Response text
        from_cache: Whether response is from cache

    Returns:
        tuple: (status_code, text, from_cache) matching _get_response_text return
    """
    return (status_code, text, from_cache)
