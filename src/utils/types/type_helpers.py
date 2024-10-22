"""Helpers for working with types and type-checking."""

from typing import Any, Callable, Optional, Protocol, TypeAlias, TypeGuard, TypeVar

T = TypeVar("T")
X = TypeVar("X")
Y = TypeVar("Y")


def elements_are(elems: list[Any], query_type: type[T]) -> TypeGuard[list[T]]:
    """Check if all elements in a list are of a certain type."""
    return all(isinstance(elem, query_type) for elem in elems)


def is_list_of(obj: Any, query_type: type[T]) -> TypeGuard[list[T]]:
    """Check if an object is a list with elements of a certain type."""

    return isinstance(obj, list) and all(isinstance(entry, query_type) for entry in obj)


Predicate: TypeAlias = Callable[[T], bool]


def all_true(predicates: list[Predicate[T]]) -> Predicate[T]:
    """Compose a list of predicates into a single predicate.

    The composed predicate will return True if all of the predicates in the list
    return True for the given argument.
    """

    def all_preds_true(obj: T) -> bool:
        return all(predicate(obj) for predicate in predicates)

    return all_preds_true


class Stringable(Protocol):
    """Protocol for objects that can be converted to a string."""

    def __str__(self) -> str: ...
