"""Utility functions for working with containers."""

from typing import Any, Iterable

from src.utils.types.type_helpers import Stringable


def subdict(full_dict: dict[str, Any], keys: list[str]) -> dict[str, Any]:
    """Return a sub-dictionary of the full dictionary, containing only values
    specified in 'keys'
    """
    return {key: full_dict[key] for key in keys}


# pylint: disable=invalid-name
class lazyjoin:
    """Class used to lazily join strings together.

    For example, to log a list of items only if the log level is set to DEBUG:
    logger.debug(lazyjoin(", ", items))
    """

    def __init__(self, s: str, items: Iterable[Stringable]):
        self.s = s
        self.items = items

    def __str__(self) -> str:
        return self.s.join((str(item) for item in self.items))
