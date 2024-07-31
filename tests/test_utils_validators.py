"""Tests for the validators module."""

# pylint: disable=missing-function-docstring
# nosec assert_used
# flake8: noqa S101

from datetime import datetime

import pytest
from dateutil import tz

from src.utils.validators import is_hex, validate_isodatetime

NZT = tz.gettz("Pacific/Auckland")


def test_is_hex_valid_hex() -> None:
    """Test is_hex function with valid input."""
    assert is_hex("0")
    assert is_hex("1")
    assert is_hex("a")
    assert is_hex("f")
    assert is_hex("A")
    assert is_hex("F")
    assert is_hex("ff")
    assert is_hex("FF")
    assert is_hex("0f")

    assert is_hex("0x0")
    assert is_hex("0X0")
    assert is_hex("0x0f")
    assert is_hex("0X0F")

    assert is_hex("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF")
    assert is_hex("04578945702952854684859406729090019481057396748385407482570582857")


def test_is_hex_invalid_hex() -> None:
    """Test is_hex function with invalid input."""
    assert not is_hex("g")
    assert not is_hex("G")
    assert not is_hex("z")
    assert not is_hex("Z")
    assert not is_hex("0x")
    assert not is_hex("0X")
    assert not is_hex("0fG")
    assert not is_hex("0FG")
    assert not is_hex("0x0g")
    assert not is_hex("0X0g")
    assert not is_hex("abcdefABCDEF0123456789g")


@pytest.mark.parametrize(
    "valid_iso_dt",
    [
        "2022-01-01T12:00:00",
        "2022-01-01T12:00:00+12:00",
        "2022-01-01T12:00:00.0+12:00",
        "2022-01-01T12:00:00.00+12:00",
        "2022-01-01T12:00:00.000+12:00",
        "2022-01-01T12:00:00.0000+12:00",
        "2022-01-01T12:00:00.00000+12:00",
        "2022-01-01T12:00:00.000000+12:00",
        datetime(2022, 1, 1, 12, 00, 00, 000000).isoformat(),
        datetime(2022, 1, 1, tzinfo=NZT).isoformat(),
        datetime(2022, 1, 1, 12, 00, 00, tzinfo=NZT).isoformat(),
    ],
)
def test_good_ISO_DateTime_string(  # pylint: disable=invalid-name
    valid_iso_dt: str,
) -> None:
    assert validate_isodatetime(valid_iso_dt) == valid_iso_dt
