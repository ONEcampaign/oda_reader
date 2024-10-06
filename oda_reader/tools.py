"""Additional tools for the API wrapper"""
from collections import OrderedDict


def get_available_filters(source: str, quiet: bool = False) -> dict:
    """
    Get the available filters for a given source (printed and as a dictionary).
    It can be "dac1", "dac2a", "multisystem", or "crs".

    Args:
        source: The source to get the filters for.
        quiet: Whether to suppress the printed output.

    Returns:
        dict: The available filters.
    """
    from oda_reader import QueryBuilder as qb
    from pprint import pprint

    match source:
        case "dac1":
            f = qb.build_dac1_filter.__annotations__
        case "dac2a":
            f = qb.build_dac2a_filter.__annotations__
        case "multisystem":
            f = qb.build_multisystem_filter.__annotations__
        case "crs":
            f = qb.build_crs_filter.__annotations__
        case _:
            raise ValueError(f"Source '{source}' not recognized.")

    available_filters = OrderedDict((k, v) for k, v in f.items() if k != "return")

    if not quiet:
        pprint(available_filters)

    return available_filters
