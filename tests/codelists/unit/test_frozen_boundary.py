"""Structural tests for the frozen boundary around `oda_reader.codelists`.

The threat is convention, not malice: every existing submodule is eagerly imported at the
top of `src/oda_reader/__init__.py` and added to `__all__`, and a future implementer
touching that file will follow the pattern unless something stops them. These two tests are
that stop:

1. A subprocess regression test asserting the *property* — `import oda_reader` opens no socket
   and resolves no hostname — rather than the convention (the module merely isn't listed).
2. A static `ast` walk asserting the translation path (`schemas/`, `download/`) never imports
   `codelists`, so extraction can never leak into a `convert_*` call.

Both are first-class deliverables, not test hygiene.
"""

from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[3]
_SRC_ROOT = _REPO_ROOT / "src" / "oda_reader"

# ---------------------------------------------------------------------------
# Test 1 — the subprocess network guard
# ---------------------------------------------------------------------------
#
# Runs in a subprocess because pytest's own session will already have imported
# `oda_reader.codelists` for the other tests in this suite — an in-process assertion on
# sys.modules would be meaningless.
#
# The sys.addaudithook half is critical: patching socket.socket catches a socket being
# constructed, but DNS resolution goes through socket.getaddrinfo, a module-level function
# that reaches C without touching the patched class. A module that resolved a hostname at
# import time would sail straight past a socket-only guard.
_GUARD = """
import socket, sys

class _Blocked(socket.socket):
    def __init__(self, *a, **k):
        raise AssertionError("import oda_reader opened a socket")

socket.socket = _Blocked

def _audit(event, args):
    if event in ("socket.getaddrinfo", "socket.connect"):
        raise AssertionError(f"import oda_reader performed network activity: {event}")

sys.addaudithook(_audit)
import oda_reader

assert "oda_reader.codelists" not in sys.modules, "codelists was imported eagerly"
assert "codelists" not in oda_reader.__all__, "codelists leaked into __all__"
assert hasattr(oda_reader, "CodelistShapeError"), "exceptions must be catchable"
"""


@pytest.mark.unit
def test_import_oda_reader_is_network_free_and_excludes_codelists() -> None:
    result = subprocess.run(
        [sys.executable, "-c", _GUARD], capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, result.stderr


# ---------------------------------------------------------------------------
# Test 2 — the ast walk
# ---------------------------------------------------------------------------
#
# A rule without enforcement is just a comment. This walks every .py file under schemas/ and
# download/, parses it with ast, and fails on the line an offending import was added.


def _module_names_codelists(module: str | None, *, absolute: bool) -> bool:
    """True if `module` (an ImportFrom's dotted module string) names codelists.

    `absolute=True` checks for `oda_reader.codelists` (and submodules of it); `absolute=False`
    checks for the relative equivalent, `codelists` (and submodules of it) at any `level > 0`.
    """
    target = "oda_reader.codelists" if absolute else "codelists"
    return module == target or (module is not None and module.startswith(f"{target}."))


def _imports_codelists(node: ast.Import | ast.ImportFrom) -> bool:
    """True if `node` imports `oda_reader.codelists`, absolute or relative."""
    if isinstance(node, ast.Import):
        return any(
            _module_names_codelists(alias.name, absolute=True) for alias in node.names
        )

    # ast.ImportFrom. Catches `from oda_reader.codelists import X` / `from ..codelists import X`
    # (module names the target directly) and `from oda_reader import codelists` /
    # `from .. import codelists` (module names the parent, codelists is one of the names).
    absolute = node.level == 0
    if _module_names_codelists(node.module, absolute=absolute):
        return True
    parent = "oda_reader" if absolute else None
    return node.module == parent and any(
        alias.name == "codelists" for alias in node.names
    )


def _find_codelist_imports(root: Path) -> list[str]:
    violations: list[str] = []
    for path in sorted(root.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        violations.extend(
            f"{path.relative_to(_REPO_ROOT)}:{node.lineno}"
            for node in ast.walk(tree)
            if isinstance(node, ast.Import | ast.ImportFrom)
            and _imports_codelists(node)
        )
    return violations


@pytest.mark.unit
def test_translation_path_never_imports_codelists() -> None:
    violations = [
        *_find_codelist_imports(_SRC_ROOT / "schemas"),
        *_find_codelist_imports(_SRC_ROOT / "download"),
    ]
    assert not violations, (
        "oda_reader.codelists must never be imported from schemas/ or download/ "
        f"(the translation path is the frozen surface): {violations}"
    )


# ---------------------------------------------------------------------------
# Test 3 — the id vocabulary is public, not just importable from `_types`
# ---------------------------------------------------------------------------
#
# `_fetch.py`'s validators print these tuples in their error messages, so the
# domain they describe is plainly user-facing. Before this test, the only way
# to obtain it programmatically was to reach into `_types`, a private module
# by this package's underscore convention. These constants must be reachable
# from `oda_reader.codelists` itself, listed in `__all__`, and immutable —
# they are the public spelling of a domain, not a list a caller can mutate.

_PUBLIC_DOMAIN_CONSTANTS = (
    "SUPPORTED_CODELIST_IDS",
    "SUPPORTED_CATEGORY_IDS",
    "STATUS_DOMAIN",
    "PRESENCE_DOMAIN",
    "LINEAGE_COLUMNS",
)


# ---------------------------------------------------------------------------
# Test 4 — the agency contract's public surface
# ---------------------------------------------------------------------------
#
# fetch_provider_agencies / parse_provider_agencies / SUPPORTED_AGENCY_IDS are
# the third contract's entry points, added alongside the area and category
# pairs above. Listed in __all__ like every other public name this package
# exports; a future refactor that quietly drops one from __all__ while leaving
# the function importable is the kind of regression a plain import wouldn't
# catch.


@pytest.mark.unit
def test_agency_contract_exports_are_public() -> None:
    from oda_reader import codelists

    for name in (
        "fetch_provider_agencies",
        "parse_provider_agencies",
        "SUPPORTED_AGENCY_IDS",
    ):
        assert name in codelists.__all__, f"{name} missing from codelists.__all__"
        assert hasattr(codelists, name), f"{name} not reachable from codelists"

    # A tuple, not a bare str, even though it only ever holds one id: uniform
    # with SUPPORTED_CODELIST_IDS / SUPPORTED_CATEGORY_IDS so generic code
    # over all three can't iterate a bare "16" character-by-character.
    assert codelists.SUPPORTED_AGENCY_IDS == ("16",)
    assert isinstance(codelists.SUPPORTED_AGENCY_IDS, tuple)


@pytest.mark.unit
@pytest.mark.parametrize("name", _PUBLIC_DOMAIN_CONSTANTS)
def test_domain_constant_is_public_and_immutable(name: str) -> None:
    from oda_reader import codelists

    assert name in codelists.__all__, f"{name} missing from codelists.__all__"
    value = getattr(codelists, name)
    assert isinstance(value, tuple), (
        f"{name} must be an immutable tuple, not {type(value).__name__}"
    )
