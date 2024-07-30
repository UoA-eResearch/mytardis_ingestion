"""Tests for the common_types module."""

import pytest

from src.mytardis_client.common_types import MD5Sum, is_hex


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
