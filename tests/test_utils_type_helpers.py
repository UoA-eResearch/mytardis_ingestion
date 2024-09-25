"""Tests for the type helpers module."""

# pylint: disable=missing-function-docstring

from src.utils.types.type_helpers import passthrough_none


def test_forward_none() -> None:

    @passthrough_none
    def double_val(value: int | float | str) -> int | float | str:
        """Double a value."""
        return value * 2

    assert double_val(2) == 4
    assert double_val(2.0) == 4.0
    assert double_val("2") == "22"
    assert double_val(None) is None
