"""Unit tests for the package's loggers and the deprecated `oda_importer` name."""

import importlib
import logging
import pkgutil
import subprocess
import sys
import textwrap
import warnings

import pytest

import oda_reader
from oda_reader._logging import _warn_once

# A module that logs at INFO during a normal download, so a message from it
# stands in for the package's progress messages.
PACKAGE_MODULE_LOGGER = "oda_reader.download.download_tools"


def _run_in_fresh_interpreter(code: str) -> str:
    """Run `code` in a new Python process, so logging state can't leak between tests."""
    result = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    return result.stdout


def _package_modules() -> list[str]:
    return [
        module.name
        for module in pkgutil.walk_packages(oda_reader.__path__, "oda_reader.")
    ]


class _RecordingHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__(level=logging.DEBUG)
        self.messages: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.messages.append(record.getMessage())


@pytest.fixture
def legacy_logger():
    """The `oda_importer` logger, with its state and the warning flag restored after."""
    legacy = logging.getLogger("oda_importer")
    saved_level = legacy.level
    saved_handlers = list(legacy.handlers)
    saved_filters = list(legacy.filters)
    saved_propagate = legacy.propagate
    _warn_once.cache_clear()
    yield legacy
    # The base-class method skips the deprecation warning.
    logging.Logger.setLevel(legacy, saved_level)
    legacy.handlers = saved_handlers
    legacy.filters = saved_filters
    legacy.propagate = saved_propagate
    _warn_once.cache_clear()


@pytest.fixture
def root_handler():
    handler = _RecordingHandler()
    logging.getLogger().addHandler(handler)
    yield handler
    logging.getLogger().removeHandler(handler)


@pytest.mark.unit
class TestImport:
    def test_import_leaves_root_logger_and_package_loggers_unconfigured(self):
        output = _run_in_fresh_interpreter(
            f"""
            import importlib
            import logging

            root = logging.getLogger()
            before = (list(root.handlers), root.level)
            for name in {_package_modules()!r}:
                importlib.import_module(name)
            assert (list(root.handlers), root.level) == before

            checked = []
            for name, item in logging.Logger.manager.loggerDict.items():
                if name.startswith(("oda_reader", "oda_importer")) and isinstance(
                    item, logging.Logger
                ):
                    assert not item.handlers, name
                    assert item.level == logging.NOTSET, name
                    assert item.propagate, name
                    checked.append(name)
            print(len(checked))
            """
        )

        assert int(output) > 0

    def test_every_module_logger_is_named_for_its_module(self):
        module_loggers = {}
        for name in _package_modules():
            module = importlib.import_module(name)
            if isinstance(logger := getattr(module, "logger", None), logging.Logger):
                module_loggers[name] = logger.name

        assert module_loggers
        assert {name: name for name in module_loggers} == module_loggers


@pytest.mark.unit
class TestDeprecatedName:
    @pytest.mark.parametrize(
        "configure",
        [
            lambda legacy: legacy.setLevel(logging.INFO),
            lambda legacy: legacy.addHandler(logging.NullHandler()),
            lambda legacy: legacy.addFilter(logging.Filter()),
        ],
        ids=["setLevel", "addHandler", "addFilter"],
    )
    def test_configuring_warns_and_names_the_new_logger(self, legacy_logger, configure):
        with pytest.warns(DeprecationWarning, match="'oda_reader'.*2.0"):
            configure(legacy_logger)

    def test_warns_once_per_process(self, legacy_logger):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            legacy_logger.setLevel(logging.INFO)
            legacy_logger.setLevel(logging.DEBUG)

        assert len(caught) == 1

    def test_level_applies_to_package_records(self, legacy_logger, root_handler):
        with pytest.warns(DeprecationWarning):
            legacy_logger.setLevel(logging.INFO)

        logging.getLogger(PACKAGE_MODULE_LOGGER).info("progress")

        assert root_handler.messages == ["progress"]

    def test_package_level_takes_precedence(self, legacy_logger, root_handler):
        package_logger = logging.getLogger("oda_reader")
        with pytest.warns(DeprecationWarning):
            legacy_logger.setLevel(logging.INFO)
        package_logger.setLevel(logging.WARNING)
        try:
            logging.getLogger(PACKAGE_MODULE_LOGGER).info("progress")
        finally:
            package_logger.setLevel(logging.NOTSET)

        assert root_handler.messages == []

    def test_handler_without_propagation_takes_records_from_root(
        self, legacy_logger, root_handler
    ):
        legacy_handler = _RecordingHandler()
        with pytest.warns(DeprecationWarning):
            legacy_logger.addHandler(legacy_handler)
            legacy_logger.setLevel(logging.INFO)
        legacy_logger.propagate = False

        logging.getLogger(PACKAGE_MODULE_LOGGER).info("progress")

        assert legacy_handler.messages == ["progress"]
        assert root_handler.messages == []

    def test_child_of_deprecated_name_reaches_root(self, root_handler):
        logging.getLogger("oda_importer.child").warning("from a child")

        assert root_handler.messages == ["from a child"]


@pytest.mark.unit
class TestApplicationConfiguration:
    def test_dict_config_naming_oda_reader_keeps_package_enabled(self):
        """dictConfig's default `disable_existing_loggers=True` disables the
        unnamed `oda_importer`, which must not silence the package logger.
        Module loggers escape the sweep as its children, so a record created on
        the package logger itself is the one that shows it."""
        output = _run_in_fresh_interpreter(
            f"""
            import logging
            import logging.config
            import sys

            import oda_reader

            logging.config.dictConfig({{
                "version": 1,
                "handlers": {{
                    "out": {{"class": "logging.StreamHandler", "stream": "ext://sys.stdout"}},
                }},
                "loggers": {{"oda_reader": {{"level": "INFO", "handlers": ["out"]}}}},
            }})
            logging.getLogger({PACKAGE_MODULE_LOGGER!r}).info("from a module")
            logging.getLogger("oda_reader").info("from the package")
            """
        )

        assert output.splitlines() == ["from a module", "from the package"]

    def test_deprecated_name_configured_before_import_still_applies(self):
        output = _run_in_fresh_interpreter(
            f"""
            import logging
            import sys

            legacy = logging.getLogger("oda_importer")
            legacy.addHandler(logging.StreamHandler(sys.stdout))
            legacy.setLevel(logging.INFO)
            legacy.propagate = False

            import oda_reader

            logging.getLogger({PACKAGE_MODULE_LOGGER!r}).info("progress")
            """
        )

        assert output.splitlines() == ["progress"]
