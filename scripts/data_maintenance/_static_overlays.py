"""Hand-maintained, source-less SDMX rules; never scraped.

These constants are version-controlled overlays applied on top of the
area codelists fetched from the OECD codelist app.  They must match the
committed mapping files in src/oda_reader/schemas/mappings/ exactly.
"""

# dac1_codes_prices.json
DAC1_PRICES: dict[str, str] = {"V": "A", "Q": "D"}

# dac1_codes_flow_types.json — includes the SDMX regex passthrough rule
DAC1_FLOW_TYPES: dict[str, str] = {"115": "C", "112": "D", "(.*)": "\\1"}

# Appended as the last entry of every rendered area map so that unknown
# codes pass through unchanged in the SDMX mapping engine.
AREA_PASSTHROUGH: dict[str, str] = {"(.*)": "\\1"}
