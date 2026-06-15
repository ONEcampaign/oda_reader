import json
from pathlib import Path

from oda_reader.common import logger


def keys_to_int(dictionary: dict) -> dict:
    """Convert dictionary keys to integers.

    Args:
        dictionary: A dictionary which has integer keys as strings.

    Returns:
        dict: The dictionary with integer keys.

    """
    return {int(k): v for k, v in dictionary.items() if k.isdigit()}


def read_mapping(mapping_path: str | Path, *, keys_as_int: bool) -> dict:
    """Read a JSON mapping file from disk.

    Args:
        mapping_path: Path to the JSON file.
        keys_as_int: When True, convert string digit keys to integers via
            :func:`keys_to_int`.

    Returns:
        dict: The loaded mapping, optionally with integer keys.

    Raises:
        FileNotFoundError: If *mapping_path* does not exist.  The refresh
            tool ``scripts/data_maintenance/refresh_dac_codelists.py`` is
            the authoritative path for regenerating missing mapping files.
    """
    path = Path(mapping_path)
    if not path.exists():
        raise FileNotFoundError(
            f"Mapping file not found: {path!r}.  "
            "Run scripts/data_maintenance/refresh_dac_codelists.py --write to regenerate."
        )

    logger.debug(f"Reading mapping from {path}")
    with open(path) as f:
        mapping = json.load(f)

    if keys_as_int:
        mapping = keys_to_int(mapping)

    return mapping
