"""Shared test helpers for tests/codelists/unit/.

The OECD envelope shape and fixture-loading skip behaviour live here so they can
be reused across multiple tests. `test_fetch.py` keeps its own fixture loader
(it needs HTML fixtures as text, not JSON fixtures as bytes) but shares `envelope`.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent.parent.parent / "fixtures" / "oecd"


def load_fixture(filename: str) -> bytes:
    path = FIXTURES / filename
    if not path.exists():
        pytest.skip(
            f"Fixture {filename} not yet captured — run --capture-fixtures first"
        )
    return path.read_bytes()


def envelope(name: str, *items: dict) -> bytes:
    """Wrap codelist-item dicts in the OECD JSON envelope shape."""
    return json.dumps(
        {
            "codelists": {
                "date-last-modified": "2026-01-01",
                "codelist": [
                    {"name": name, "codelist-items": {"codelist-item": list(items)}}
                ],
            }
        }
    ).encode()
