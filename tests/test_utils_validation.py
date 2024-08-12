"""Tests for the validators module."""

# pylint: disable=missing-function-docstring
# nosec assert_used
# flake8: noqa S101

from datetime import datetime

import pytest
from dateutil import tz

from src.utils.validation import is_hex, validate_isodatetime, validate_md5sum
from hypothesis import given, strategies as st

NZT = tz.gettz("Pacific/Auckland")

HEXREGEX = r"(^(0x)?[0-9a-fA-F]+)"
MD5REGEX = r"(^([A-Fa-f0-9]{32}))"
BAD_MD5REGEX = r"(^((.{0,31})|(.{33,})|([G-Zg-z]{32})|(\W{32})))"

@given(st.from_regex(HEXREGEX,fullmatch=True))
def test_is_hex_valid_hex(value:str) -> None:
    """Test is_hex function with valid input."""
    assert is_hex(value=value)
    
def test_is_hex_invalid_hex(value:str) -> None:
    """Test is_hex function with invalid input."""
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


@given(st.from_regex(MD5REGEX,fullmatch=True))
def test_md5sum_valid(value: str) -> None:
    """Test that a valid MD5Sum is accepted and serialized correctly."""
    assert validate_md5sum(value) == value

@given(st.from_regex(BAD_MD5REGEX,fullmatch=True))
def test_md5sum_invalid(value: str) -> None:
    """Test that an invalid MD5Sum is rejected."""

    with pytest.raises(ValueError):
        _ = validate_md5sum(value)


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


@given(st.datetimes())
def test_hypothesis_ISO_DateTime_string(  # pylint: disable=invalid-name
    valid_iso_dt: datetime,
) -> None:
    valid_iso_dt.isoformat()
    assert validate_isodatetime(valid_iso_dt.isoformat()) == valid_iso_dt.isoformat()
