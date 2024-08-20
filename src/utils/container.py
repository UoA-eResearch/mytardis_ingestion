"""Utility functions for working with containers."""

from typing import Any


def subdict(full_dict: dict[str, Any], keys: list[str]) -> dict[str, Any]:
    """Return a sub-dictionary of the full dictionary, containing only values
    specified in 'keys'
    """
    return {key: full_dict[key] for key in keys}
