"""Characterization tests for the cache-directory resolution layer.

Pins the behaviour of ``_cache/config.py::get_cache_dir`` (which delegates
to ``readerkit.resolve_cache_dir``) that must not change: precedence
between the programmatic override, the ``ODA_READER_CACHE_DIR`` env var,
and the platform default; ``reset_cache_dir()`` restoring the env-var
value; the change-listener notification; that the derived HTTP/bulk/dataframe
sub-paths keep nesting under ``get_cache_dir()``; and the exact on-disk
layout (``<root>/v<schema>/oda-reader/<version>``).

The redirect assertions check root *membership* (``is_relative_to``) rather
than exact equality: ``readerkit.resolve_cache_dir`` always appends its own
schema/slug/version segments beneath whichever root wins (override, env var,
or default), so the resolved directory is never byte-identical to the root
that redirected it — only nested under it. That nesting, not exact equality,
is the invariant this module relies on, as documented in
``resolve_cache_dir``'s own docstring in readerkit — not inferred from what
this test happens to produce.
"""

from importlib.metadata import version

import pytest
from platformdirs import user_cache_dir
from readerkit import CACHE_SCHEMA_VERSION

from oda_reader._cache import config as cache_config
from oda_reader._cache.config import (
    get_bulk_cache_dir,
    get_cache_dir,
    get_dataframe_cache_dir,
    get_http_cache_path,
    register_cache_dir_change_listener,
    reset_cache_dir,
    set_cache_dir,
)


@pytest.fixture(autouse=True)
def _reset_override(monkeypatch):
    """Every test in this module starts and ends with no override active, and
    with neither cache-dir env var set, so a developer's or CI's ambient
    environment cannot leak into the resolution being tested.
    """
    monkeypatch.delenv("ODA_READER_CACHE_DIR", raising=False)
    monkeypatch.delenv("BBLOCKS_CACHE_DIR", raising=False)
    reset_cache_dir()
    yield
    reset_cache_dir()


def test_env_var_redirects_the_cache_root(monkeypatch, tmp_path):
    """ODA_READER_CACHE_DIR wins over the platform default.

    Asserts against ``target`` directly, not ``target.resolve()``: readerkit's
    root resolution uses ``os.path.abspath``, which does not follow symlinks,
    so comparing against a canonicalized path would only pass by coincidence
    on a temp root that happens to already be canonical. ``tmp_path`` is
    already absolute, so no normalization is needed on the expected side
    either.
    """
    target = tmp_path / "env-cache"
    monkeypatch.setenv("ODA_READER_CACHE_DIR", str(target))

    assert get_cache_dir().is_relative_to(target)


def test_set_cache_dir_wins_over_env_var(monkeypatch, tmp_path):
    """A programmatic override takes precedence over the env var."""
    env_target = tmp_path / "env-cache"
    monkeypatch.setenv("ODA_READER_CACHE_DIR", str(env_target))
    override = tmp_path / "override-cache"

    set_cache_dir(override)
    result = get_cache_dir()

    assert result.is_relative_to(override.resolve())
    assert not result.is_relative_to(env_target.resolve())


def test_reset_cache_dir_restores_the_env_var_value(monkeypatch, tmp_path):
    """reset_cache_dir() clears the override and falls back to the env var.

    Asserts against ``env_target`` directly rather than its resolved form —
    see ``test_env_var_redirects_the_cache_root`` for why: readerkit does not
    canonicalize symlinks, so this pins that behaviour rather than an
    incidental property of pytest's temp directory.
    """
    env_target = tmp_path / "env-cache"
    monkeypatch.setenv("ODA_READER_CACHE_DIR", str(env_target))
    set_cache_dir(tmp_path / "override-cache")

    reset_cache_dir()

    assert get_cache_dir().is_relative_to(env_target)


def test_get_cache_dir_falls_back_to_platform_default():
    """With neither an override nor either cache-dir env var set,
    get_cache_dir() falls through to readerkit's own platform cache dir
    (platformdirs.user_cache_dir("readerkit", appauthor=False)) rather than
    oda_reader's pre-swap ``user_cache_dir("oda-reader", "oda-reader")``.
    That base changing is exactly the documented, intentional consequence of
    adopting readerkit (every existing user's cache is orphaned once); this
    assertion pins the new base rather than letting it drift unnoticed.
    """
    result = get_cache_dir()

    expected_base = user_cache_dir("readerkit", appauthor=False)
    assert result.is_relative_to(expected_base)


def test_listener_fires_on_redirect(tmp_path, monkeypatch):
    """register_cache_dir_change_listener callbacks fire on set_cache_dir and
    on reset_cache_dir. Isolated onto a private listener list so this test
    does not trigger the real dataframe/bulk-cache listeners registered by
    other modules at import time.
    """
    monkeypatch.setattr(cache_config, "_CACHE_DIR_LISTENERS", [])
    calls = []
    register_cache_dir_change_listener(lambda: calls.append(1))

    set_cache_dir(tmp_path / "a")
    reset_cache_dir()

    assert calls == [1, 1]


def test_downstream_paths_nest_under_get_cache_dir(tmp_path):
    """get_http_cache_path / get_bulk_cache_dir / get_dataframe_cache_dir
    are always subdirectories of whatever get_cache_dir() currently
    resolves to.
    """
    set_cache_dir(tmp_path / "root")

    root = get_cache_dir()

    assert get_http_cache_path() == root / "http_cache"
    assert get_bulk_cache_dir() == root / "bulk_files"
    assert get_dataframe_cache_dir() == root / "dataframes"


def test_get_cache_dir_pins_exact_layout(tmp_path):
    """get_cache_dir() resolves to exactly
    ``<root>/v<CACHE_SCHEMA_VERSION>/oda-reader/<oda_reader version>`` — not
    merely somewhere under the configured root. The other tests in this
    module only assert containment (``is_relative_to``); none of them would
    catch readerkit dropping the app_version segment, changing the app slug,
    or bumping CACHE_SCHEMA_VERSION. Importing CACHE_SCHEMA_VERSION from
    readerkit means a schema bump fails this assertion instead of silently
    relocating every user's cache.
    """
    override = tmp_path / "root"
    set_cache_dir(override)

    expected = (
        override.resolve()
        / f"v{CACHE_SCHEMA_VERSION}"
        / "oda-reader"
        / version("oda_reader")
    )
    assert get_cache_dir() == expected
