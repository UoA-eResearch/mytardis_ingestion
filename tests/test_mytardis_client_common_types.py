"""Tests for the common_types module."""

import pytest

from src.mytardis_client.common_types import MD5Sum


@pytest.mark.parametrize(
    "value",
    [
        "0123456789abcdef0123456789abcdef",
        "0123456789ABCDEF0123456789ABCDEF",
        "ffffffffffffffffffffffffffffffff",
        "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF",
        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        "00000000000000000000000000000000",
    ],
)
def test_md5sum_valid(value: str) -> None:
    """Test that a valid MD5Sum is accepted and serialized correctly."""
    md5sum = MD5Sum(value)
    assert str(md5sum) == value


@pytest.mark.parametrize(
    "value",
    [
        "0123456789abcdef0123456789abcdefa",
        "0123456789ABCDEF0123456789ABCDEFA",
        "fffffffffffffffffffffffffffffff",
        "fffffffffffffffffffffffffffffffff",
        "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF",
        "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF",
        "AAAAA",
        "00000",
        "A",
        "F",
        "0",
        "&*%^%$%^$%&*&^()",
        "banana",
    ],
)
def test_md5sum_invalid(value: str) -> None:
    """Test that an invalid MD5Sum is rejected."""

    with pytest.raises(ValueError):
        _ = MD5Sum(value)
