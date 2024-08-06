"""Utility functions for validating data."""

from typing import Any

from dateutil import parser
from validators import url


def is_hex(value: str) -> bool:
    """Check if a string is a valid hexadecimal string."""
    try:
        _ = int(value, 16)
    except ValueError:
        return False

    return True


def validate_md5sum(value: str) -> str:
    """Check that the input string is a well-formed MD5Sum."""
    if len(value) != 32:
        raise ValueError("MD5Sum must contain exactly 32 characters")
    if not is_hex(value):
        raise ValueError("MD5Sum must be a valid hexadecimal string")

    return value


def validate_isodatetime(value: Any) -> str:
    """Custom validator to ensure that the value is a string object and that it matches
    the regex defined for an ISO 8601 formatted datetime string"""
    if not isinstance(value, str):
        raise TypeError(f'Unexpected type for ISO date/time stamp: "{type(value)}"')

    try:
        _ = parser.isoparse(value)
    except ValueError as e:
        raise ValueError(
            f'Passed string value "{value}" is not an ISO 8601 formatted '
            "date/time string. Format should follow "
            "YYYY-MM-DDTHH:MM:SS.SSSSSS+HH:MM convention"
        ) from e

    return value


def validate_url(value: Any) -> str:
    """Custom validator for Urls since the default pydantic ones are not compatible
    with urllib"""
    if not isinstance(value, str):
        raise TypeError(f'Unexpected type for URL: "{type(value)}"')
    if url(value):
        return value
    raise ValueError(f'Passed string value"{value}" is not a valid URL')
