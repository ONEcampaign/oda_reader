# ODA Reader Test Suite

This directory contains the test suite for the ODA Reader package.

## Structure

```
tests/
├── common/          # Core utilities (rate limiter, cache)
│   └── unit/        # Unit tests for common functionality
├── download/        # Download layer (query builder, API calls)
│   └── unit/        # Unit tests for download tools
├── schemas/         # Schema translation
│   └── unit/        # Unit tests for schema translation
├── datasets/        # Dataset-specific tests
│   ├── dac1/        # DAC1 dataset tests
│   ├── dac2a/       # DAC2a dataset tests
│   └── crs/         # CRS dataset tests
│       └── integration/  # End-to-end integration tests
├── fixtures/        # Test data and mock responses
│   ├── api_responses/
│   └── expected_outputs/
├── conftest.py      # Shared fixtures
└── utils.py         # Test utilities
```

Pytest configuration is in `pyproject.toml` under `[tool.pytest.ini_options]`.

## Running Tests

### Run all unit tests (default, fast)
```bash
uv run pytest tests/
```

By default, integration tests are excluded to keep test runs fast. Only unit tests with mocked dependencies will run.

### Run all tests including integration
```bash
uv run pytest tests/ -m ""
```

This will run the full suite, including integration tests that make real API calls.

### Run only integration tests
```bash
uv run pytest tests/ -m integration
```

### Run only unit tests (explicit)
```bash
uv run pytest tests/ -m unit
```

### Run specific test file
```bash
uv run pytest tests/common/unit/test_rate_limiter.py -v
```

### Run specific test class or function
```bash
# Run a specific test class
uv run pytest tests/common/unit/test_rate_limiter.py::TestRateLimiterBlocking -v

# Run a specific test function
uv run pytest tests/common/unit/test_rate_limiter.py::TestRateLimiterBlocking::test_blocks_when_limit_exceeded -v
```

### Run with coverage
```bash
uv run pytest tests/ --cov=src/oda_reader --cov-report=html
```

This generates an HTML coverage report in `htmlcov/index.html`.

### Run in parallel (unit tests only)
```bash
uv run pytest tests/ -n auto -m "not integration"
```

This uses all available CPU cores to run unit tests in parallel for faster execution.

## Test Markers

Tests are organized using pytest markers to control execution:

- `@pytest.mark.unit` - Fast unit tests with mocked dependencies (default)
- `@pytest.mark.integration` - Tests that call real OECD API (skipped by default)
- `@pytest.mark.slow` - Long-running tests (bulk downloads, extensive processing)
- `@pytest.mark.cache` - Tests that verify cache behavior

### Using Markers

```python
import pytest

@pytest.mark.unit
def test_query_builder():
    """Fast unit test with no external dependencies."""
    pass

@pytest.mark.integration
def test_real_api_call():
    """Integration test that hits real OECD API."""
    pass
```

## Writing Tests

### Unit Tests

Unit tests should:
- Mock external dependencies (HTTP calls, file I/O)
- Be fast (<100ms per test)
- Test business logic in isolation
- Use parametrization for comprehensive coverage
- Focus on edge cases and error handling

Example:
```python
import pytest

@pytest.mark.unit
def test_query_builder_filter(mocker):
    """Test query builder creates correct filter strings."""
    from oda_reader.download.query_builder import QueryBuilder

    qb = QueryBuilder(dataflow_id="DF_DAC1", api_version=1)
    result = qb.build_dac1_filter(donor="1", measure="2010")

    assert result == "1.2010....."
```

### Integration Tests

Integration tests should:
- Use real API calls (no mocking)
- Be marked with `@pytest.mark.integration`
- Use small queries (single year, specific filters) to minimize API load
- Respect rate limits
- Enable HTTP caching to avoid redundant API calls
- Test critical user-facing functionality

Example:
```python
import pytest
from oda_reader import dac1, enable_http_cache

@pytest.mark.integration
def test_dac1_basic_query():
    """Test basic DAC1 query returns valid data."""
    enable_http_cache()

    df = dac1(
        donor="1",
        start_period="2023",
        end_period="2023"
    )

    assert df is not None
    assert len(df) > 0
    assert "TIME_PERIOD" in df.columns
    assert "OBS_VALUE" in df.columns
```

### Parametrized Tests

Use parametrization to test multiple scenarios with one test function:

```python
import pytest

@pytest.mark.unit
@pytest.mark.parametrize("input_val,expected", [
    ("1", "1"),
    (["1", "2"], "1+2"),
    (None, ""),
])
def test_filter_conversion(input_val, expected):
    """Test filter string conversion handles various inputs."""
    from oda_reader.download.query_builder import QueryBuilder

    qb = QueryBuilder(dataflow_id="DF_DAC1", api_version=1)
    result = qb._to_filter_str(input_val)

    assert result == expected
```

## Fixtures

Key fixtures available in all tests (defined in `conftest.py`):

### `temp_cache_dir`
Creates a temporary cache directory for testing cache behavior.

```python
def test_cache_behavior(temp_cache_dir):
    """Test uses isolated cache directory."""
    assert temp_cache_dir.exists()
    # Cache operations here won't affect real cache
```

### `rate_limiter_fast`
Provides a fast rate limiter for testing (2 calls per 0.5 seconds).

```python
def test_rate_limiting(rate_limiter_fast):
    """Test with faster rate limiter for quick tests."""
    limiter = rate_limiter_fast
    assert limiter.max_calls == 2
    assert limiter.period == 0.5
```

### `sample_csv_response`
Returns sample CSV data for mocking API responses.

```python
def test_csv_parsing(sample_csv_response):
    """Test CSV parsing with sample data."""
    assert "DONOR,RECIPIENT" in sample_csv_response
```

### `fixtures_dir`
Returns the path to the fixtures directory.

```python
def test_load_fixture(fixtures_dir):
    """Test loading fixture files."""
    fixture_path = fixtures_dir / "api_responses" / "sample.json"
    # Load and use fixture
```

### Auto-enabled Fixtures

The `disable_cache_for_tests` fixture runs automatically for all tests, ensuring the HTTP cache is disabled by default to provide isolation between tests.

## Test Utilities

Helper functions are available in `tests/utils.py`:

### `assert_dataframe_schema(df, expected_columns)`
Validates DataFrame has expected columns and types.

```python
from tests.utils import assert_dataframe_schema

def test_dataframe_structure():
    df = get_some_dataframe()
    assert_dataframe_schema(df, {
        "TIME_PERIOD": "int64",
        "OBS_VALUE": "float64",
    })
```

### `load_json_fixture(fixtures_dir, fixture_name)`
Loads a JSON fixture file.

```python
from tests.utils import load_json_fixture

def test_with_fixture(fixtures_dir):
    data = load_json_fixture(fixtures_dir, "sample_response")
    assert data["status"] == "success"
```

### `mock_api_response(status_code, text, from_cache=False)`
Creates a mock API response tuple.

```python
from tests.utils import mock_api_response

def test_api_error_handling(mocker):
    mocker.patch(
        "oda_reader.common._get_response_text",
        return_value=mock_api_response(404, "Not found")
    )
    # Test error handling
```

## CI/CD

Tests run automatically in GitHub Actions:

### On Every Commit
- **Unit tests only** (~1-2 minutes)
- Runs on Python 3.10 - 3.13
- Must pass before merge

### On Pull Requests to Main
- **Full suite** including integration tests (~5-10 minutes)
- Runs on Python 3.12
- Lint checks with ruff

### Running Locally Before Pushing

```bash
# Quick check (unit tests only)
uv run pytest tests/ -v

# Full check (includes integration)
uv run pytest tests/ -m "" -v

# Lint check
uv run ruff check src/oda_reader/ tests/
uv run ruff format --check src/oda_reader/ tests/
```

## Test Coverage

To generate a coverage report:

```bash
# Terminal report
uv run pytest tests/ --cov=src/oda_reader --cov-report=term

# HTML report (opens in browser)
uv run pytest tests/ --cov=src/oda_reader --cov-report=html
open htmlcov/index.html
```

Coverage goals:
- Overall: >80%
- Core modules (common, download, query_builder): >90%
- Dataset modules (dac1, dac2a, crs): >70%

## Troubleshooting

### Tests are slow
By default, integration tests are skipped. If tests are still slow:
- Ensure you're running unit tests only: `uv run pytest tests/ -m "not integration"`
- Use parallel execution: `uv run pytest tests/ -n auto`

### Integration tests fail with rate limit errors
- Reduce the number of concurrent test runs
- Check that `enable_http_cache()` is called in integration tests
- Wait a minute between test runs to respect API rate limits

### Import errors
Make sure dependencies are installed:
```bash
uv sync --group test
```

### Cache-related test failures
The cache is disabled by default in tests. If you need to test cache behavior:
1. Use the `@pytest.mark.cache` marker
2. Manually enable cache in the test with `enable_http_cache()`
3. Use the `temp_cache_dir` fixture for isolation

## Best Practices

1. **Test behavior, not implementation**: Focus on what the code does, not how it does it
2. **Keep tests independent**: Each test should be able to run in isolation
3. **Use descriptive names**: Test names should clearly describe what they test
4. **Arrange-Act-Assert**: Structure tests with clear setup, execution, and verification phases
5. **Don't test the framework**: Trust that pandas, requests, etc. work correctly
6. **Mock at boundaries**: Mock HTTP calls and file I/O, not internal functions
7. **Keep integration tests focused**: Test critical paths only, use small queries

## Adding New Tests

When adding functionality, follow this pattern:

1. **Add unit tests first**: Test the new function/class in isolation
2. **Use TDD when possible**: Write failing test, implement code, verify it passes
3. **Add integration test if needed**: For user-facing features, add end-to-end test
4. **Update this README**: Document any new fixtures or utilities you create

Example workflow:
```bash
# Create test file
touch tests/download/unit/test_new_feature.py

# Write failing test
# (edit file)

# Run test to see it fail
uv run pytest tests/download/unit/test_new_feature.py -v

# Implement feature
# (edit source file)

# Run test to see it pass
uv run pytest tests/download/unit/test_new_feature.py -v

# Run full unit suite to ensure no regressions
uv run pytest tests/ -v
```
