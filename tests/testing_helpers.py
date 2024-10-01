"""Helper classes and functions for testing."""

from typing import Generic, TypeVar

X = TypeVar("X")
Y = TypeVar("Y")
Z = TypeVar("Z")


class ArgMatcher(Generic[X, Y, Z]):
    """A callable that matches arguments to a dictionary of expected values.

    Intended to be used as a side_effect for MagicMock objects.

    NOTE: currently only supports two arguments. This could potentially be
    extended to support more arguments using variadic generics.
    """

    def __init__(self, matches: dict[X, dict[Y, Z]]) -> None:
        self.matches = matches

    def __call__(self, x: X, y: Y) -> Z:
        try:
            return self.matches[x][y]
        except KeyError as e:
            raise AssertionError(f"Unexpected args: {(x, y)}") from e
