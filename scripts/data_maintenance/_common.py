"""Shared helpers for data_maintenance scripts."""

from __future__ import annotations

import difflib
import json
import sys


def dumps_canonical(mapping: dict[str, str]) -> str:
    """Render a {str_code: str_value} mapping in canonical form.

    Keys that are all-digits are sorted ascending by int value; the
    AREA_PASSTHROUGH sentinel ``{"(.*)": "\\\\1"}`` is always appended last
    (unconditionally — callers do not need to include it in *mapping*).
    Returns a JSON string with indent=2, ensure_ascii=True, trailing newline.
    """
    passthrough_key = "(.*)"
    digit_items = sorted(
        ((k, v) for k, v in mapping.items() if k != passthrough_key and k.isdigit()),
        key=lambda kv: int(kv[0]),
    )
    # Non-digit, non-passthrough keys (e.g. price codes "V", "Q") sorted lexically
    other_items = sorted(
        (k, v) for k, v in mapping.items() if k != passthrough_key and not k.isdigit()
    )
    # Preserve any explicit passthrough value from mapping; default to "\\1"
    passthrough_val = mapping.get(passthrough_key, "\\1")
    ordered: dict[str, str] = (
        dict(digit_items) | dict(other_items) | {passthrough_key: passthrough_val}
    )
    return json.dumps(ordered, indent=2, ensure_ascii=True) + "\n"


def emit_json_diff(
    original: str,
    proposed: str,
    *,
    fromfile: str,
    tofile: str,
) -> None:
    """Write a unified diff of *original* vs *proposed* to stdout."""
    sys.stdout.writelines(
        difflib.unified_diff(
            original.splitlines(keepends=True),
            proposed.splitlines(keepends=True),
            fromfile=fromfile,
            tofile=tofile,
        )
    )
