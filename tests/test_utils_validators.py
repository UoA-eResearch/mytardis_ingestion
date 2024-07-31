"""Tests for the validators module."""

from src.utils.validators import is_hex


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
