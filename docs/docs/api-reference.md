# API Reference

This page provides automatically generated API documentation for ODA Reader's public functions and classes.

## Data Download Functions

::: oda_reader.download_dac1
    options:
      show_root_heading: true
      show_source: false

::: oda_reader.download_dac2a
    options:
      show_root_heading: true
      show_source: false

::: oda_reader.download_crs
    options:
      show_root_heading: true
      show_source: false

::: oda_reader.download_multisystem
    options:
      show_root_heading: true
      show_source: false

::: oda_reader.download_aiddata
    options:
      show_root_heading: true
      show_source: false

## Bulk Download Functions

::: oda_reader.bulk_download_crs
    options:
      show_root_heading: true
      show_source: false

::: oda_reader.bulk_download_multisystem
    options:
      show_root_heading: true
      show_source: false

::: oda_reader.download_crs_file
    options:
      show_root_heading: true
      show_source: false

## Utility Functions

::: oda_reader.get_available_filters
    options:
      show_root_heading: true
      show_source: false

::: oda_reader.QueryBuilder
    options:
      show_root_heading: true
      show_source: false
      members:
        - build_dac1_filter
        - build_dac2a_filter
        - build_crs_filter
        - build_multisystem_filter

## Cache Management

::: oda_reader.get_cache_dir
    options:
      show_root_heading: true
      show_source: false

::: oda_reader.set_cache_dir
    options:
      show_root_heading: true
      show_source: false

::: oda_reader.reset_cache_dir
    options:
      show_root_heading: true
      show_source: false

::: oda_reader.clear_cache
    options:
      show_root_heading: true
      show_source: false

::: oda_reader.enable_http_cache
    options:
      show_root_heading: true
      show_source: false

::: oda_reader.disable_http_cache
    options:
      show_root_heading: true
      show_source: false

::: oda_reader.clear_http_cache
    options:
      show_root_heading: true
      show_source: false

::: oda_reader.get_http_cache_info
    options:
      show_root_heading: true
      show_source: false

## Rate Limiting

::: oda_reader.common.RateLimiter
    options:
      show_root_heading: true
      show_source: false
      heading_level: 3

The global rate limiter instance is available as:

```python
from oda_reader import API_RATE_LIMITER

# Configure rate limiting
API_RATE_LIMITER.max_calls = 20
API_RATE_LIMITER.period = 60
```

See [Caching & Performance](caching.md#rate-limiting) for more details.

## Getting Help

For issues or feature requests, visit the [GitHub repository](https://github.com/ONEcampaign/oda_reader).
