"""Keep the deprecated ``oda_importer`` logger name working until 2.0.

``oda_importer`` sits above ``oda_reader`` in the logger hierarchy, so levels,
handlers and ``propagate`` set on it still apply to every ``oda_reader.*``
record through the standard parent walk. Nothing is forwarded, so a
``dictConfig`` sweep that disables ``oda_importer`` leaves the package alone.
"""

from __future__ import annotations

import functools
import logging
import warnings
from typing import TYPE_CHECKING, override

if TYPE_CHECKING:
    from logging import _FilterType

LEGACY_LOGGER_NAME = "oda_importer"
PACKAGE_LOGGER_NAME = "oda_reader"


# Once per process: caplog and dictConfig call setLevel repeatedly.
@functools.cache
def _warn_once() -> None:
    warnings.warn(
        f"The {LEGACY_LOGGER_NAME!r} logger is deprecated; configure "
        f"{PACKAGE_LOGGER_NAME!r} instead. The old name is preserved through 1.x "
        "and removed in 2.0.",
        DeprecationWarning,
        stacklevel=3,
    )


class _DeprecatedLoggerName(logging.Logger):
    """The ``oda_importer`` logger, which warns when configured."""

    @override
    def setLevel(self, level: int | str) -> None:
        _warn_once()
        super().setLevel(level)

    @override
    def addHandler(self, hdlr: logging.Handler) -> None:
        _warn_once()
        super().addHandler(hdlr)

    @override
    def addFilter(self, filter: _FilterType) -> None:
        _warn_once()
        super().addFilter(filter)


def install_legacy_logger_name() -> None:
    """Make ``oda_importer`` the parent of the ``oda_reader`` package logger."""
    legacy = logging.getLogger(LEGACY_LOGGER_NAME)
    # Leave loggers made by an application's own logger class untouched.
    if type(legacy) is logging.Logger:
        legacy.__class__ = _DeprecatedLoggerName
    # Configuration made before the import never reached the warning methods.
    if legacy.level != logging.NOTSET or legacy.handlers or legacy.filters:
        _warn_once()
    logging.getLogger(PACKAGE_LOGGER_NAME).parent = legacy
    # Reparenting skips the isEnabledFor cache reset that setLevel performs.
    logging.Logger.manager._clear_cache()  # ty: ignore[unresolved-attribute]
