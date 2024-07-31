"""Utility functions for validating data."""

import re
from typing import Any

from validators import url


def is_hex(value: str) -> bool:
    """Check if a string is a valid hexadecimal string."""
    try:
        _ = int(value, 16)
    except ValueError:
        return False

    return True


iso_time_regex = re.compile(
    r"^(-?(?:[1-9][0-9]*)?[0-9]{4})-(1[0-2]|0[1-9])-(3[01]|0[1-9]|[12][0-9])T(2[0-3]|[01][0-9]):([0-5][0-9]):([0-5][0-9])(\.[0-9]+)?(Z|[+-](?:2[0-3]|[01][0-9]):[0-5][0-9])?$"  # pylint: disable=line-too-long
)


def validate_isodatetime(value: Any) -> str:
    """Custom validator to ensure that the value is a string object and that it matches
    the regex defined for an ISO 8601 formatted datetime string"""
    if not isinstance(value, str):
        raise TypeError(f'Unexpected type for ISO date/time stamp: "{type(value)}"')
    if match := iso_time_regex.fullmatch(value):
        return f"{match.group(0)}"
    raise ValueError(
        'Passed string value "%s" is not an ISO 8601 formatted '
        "date/time string. Format should follow "
        "YYYY-MM-DDTHH:MM:SS.SSSSSS+HH:MM convention" % (value)
    )


def validate_url(value: Any) -> str:
    """Custom validator for Urls since the default pydantic ones are not compatible
    with urllib"""
    if not isinstance(value, str):
        raise TypeError(f'Unexpected type for URL: "{type(value)}"')
    if url(value):
        return value
    raise ValueError(f'Passed string value"{value}" is not a valid URL')
