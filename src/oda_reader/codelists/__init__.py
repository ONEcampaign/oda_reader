"""OECD DAC codelists: fetch, parse and reconcile against the live source.

``codelists`` is deliberately absent from ``oda_reader/__init__.py`` and
from ``oda_reader.__all__`` — it is not imported eagerly and opens no
network connection on import. Import it explicitly:

    from oda_reader.codelists import fetch_codelists
"""

from __future__ import annotations

from oda_reader.codelists._categories import (
    fetch_code_categories,
    parse_code_categories,
)
from oda_reader.codelists._fetch import fetch_codelists
from oda_reader.codelists._parse import parse_codelists
from oda_reader.codelists._reconcile import reconcile
from oda_reader.codelists._types import CodelistSnapshot, Reconciliation

__all__: list[str] = [
    "CodelistSnapshot",
    "Reconciliation",
    "fetch_code_categories",
    "fetch_codelists",
    "parse_code_categories",
    "parse_codelists",
    "reconcile",
]
