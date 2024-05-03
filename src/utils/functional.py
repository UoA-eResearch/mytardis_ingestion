"""Functional-style utilities."""

from typing import Callable, Iterator, Optional, TypeVar

MapInput = TypeVar("MapInput")
MapOutput = TypeVar("MapOutput")


def map_optional(
    f: Callable[[MapInput], MapOutput], values: Optional[Iterator[MapInput]]
) -> Optional[list[MapOutput]]:
    """Map a function over an optional iterator.

    Returns None if the input is None, otherwise returns a list of the mapped values.
    """
    if values is None:
        return None

    return [f(value) for value in values]
