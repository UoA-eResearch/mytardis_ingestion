"""Helpers for working with types and type-checking."""

from typing import Any, Protocol, TypeGuard, TypeVar

T = TypeVar("T")


def elements_are(elems: list[Any], query_type: type[T]) -> TypeGuard[list[T]]:
    """Check if all elements in a list are of a certain type."""
    return all(isinstance(elem, query_type) for elem in elems)


def is_list_of(obj: Any, query_type: type[T]) -> TypeGuard[list[T]]:
    """Check if an object is a list with elements of a certain type."""

    return isinstance(obj, list) and all(isinstance(entry, query_type) for entry in obj)


class Stringable(Protocol):
    """Protocol for objects that can be converted to a string."""

    def __str__(self) -> str: ...
