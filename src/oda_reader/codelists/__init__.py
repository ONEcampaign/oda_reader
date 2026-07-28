"""OECD DAC codelists: fetch, parse and reconcile against the live source.

``codelists`` is not re-exported from ``oda_reader``. Import it explicitly:

    from oda_reader.codelists import fetch_codelists

Three contracts share this package. **Area**: ``fetch_codelists`` /
``parse_codelists`` cover the two codelists keyed on an entity plus
activation date (providers, recipients). **Category**: ``fetch_code_categories``
/ ``parse_code_categories`` cover the remaining flat category codelists.
**Agency**: ``fetch_provider_agencies`` / ``parse_provider_agencies`` cover
codelist 16 (Provider agency), keyed additionally on donor because an agency
code is only meaningful within its donor. All three pairs return a
``CodelistSnapshot``; feed one into ``reconcile`` to merge it against a
previous table with never-delete lineage semantics, producing a
``Reconciliation``. ``SUPPORTED_CODELIST_IDS``, ``SUPPORTED_CATEGORY_IDS``
and ``SUPPORTED_AGENCY_IDS`` state which codelist ids each contract accepts;
``STATUS_DOMAIN`` and ``PRESENCE_DOMAIN`` state the vocabularies their
output columns are drawn from; ``LINEAGE_COLUMNS`` names the four columns
``reconcile`` adds on top of a snapshot's frame.
"""

from __future__ import annotations

from oda_reader.codelists._agencies import (
    fetch_provider_agencies,
    parse_provider_agencies,
)
from oda_reader.codelists._categories import (
    fetch_code_categories,
    parse_code_categories,
)
from oda_reader.codelists._fetch import fetch_codelists
from oda_reader.codelists._parse import parse_codelists
from oda_reader.codelists._reconcile import reconcile
from oda_reader.codelists._types import (
    LINEAGE_COLUMNS,
    PRESENCE_DOMAIN,
    STATUS_DOMAIN,
    SUPPORTED_AGENCY_IDS,
    SUPPORTED_CATEGORY_IDS,
    SUPPORTED_CODELIST_IDS,
    CodelistSnapshot,
    Reconciliation,
)

__all__: list[str] = [
    "CodelistSnapshot",
    "LINEAGE_COLUMNS",
    "PRESENCE_DOMAIN",
    "Reconciliation",
    "STATUS_DOMAIN",
    "SUPPORTED_AGENCY_IDS",
    "SUPPORTED_CATEGORY_IDS",
    "SUPPORTED_CODELIST_IDS",
    "fetch_code_categories",
    "fetch_codelists",
    "fetch_provider_agencies",
    "parse_code_categories",
    "parse_codelists",
    "parse_provider_agencies",
    "reconcile",
]
