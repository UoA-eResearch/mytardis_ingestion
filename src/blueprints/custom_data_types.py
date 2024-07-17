# pylint: disable=consider-using-f-string,unnecessary-lambda
""" This module defines custom data types for use in the MyTardis ingestion scripts.
 These data types include validators and custom exceptions for accurate logging and error
handling.
"""

import re
from typing import Annotated, Any

from pydantic import AfterValidator, PlainSerializer, WithJsonSchema
from validators import url

user_regex = re.compile(
    r"^[a-z]{2,4}[0-9]{3}$"  # Define as a constant in case of future change
)


def validate_username(value: Any) -> str:
    """Defines a validated username, in other words, ensure that the username meets a standardised
    format appropriate to the institution.

    Note this is a user class defined for the University of Auckland UPI format. For
    other username formats please update the user_regex pattern.
    """
    if not isinstance(value, str):
        raise TypeError(f'Unexpected type for Username: "{type(value)}"')
    if match := user_regex.fullmatch(value.lower()):
        return f"{match.group(0)}"
    raise ValueError(f'Passed string value "{value}" is not a well formatted Username')


Username = Annotated[
    str,
    AfterValidator(validate_username),
    PlainSerializer(lambda x: str(x), return_type=str),
    WithJsonSchema({"type": "string"}, mode="serialization"),
]


def validate_url(value: Any) -> str:
    """Custom validator for Urls since the default pydantic ones are not compatible
    with urllib"""
    if not isinstance(value, str):
        raise TypeError(f'Unexpected type for URL: "{type(value)}"')
    if url(value):
        return value
    raise ValueError(f'Passed string value"{value}" is not a valid URL')


MTUrl = Annotated[
    str,
    AfterValidator(validate_url),
    PlainSerializer(lambda x: str(x), return_type=str),
    WithJsonSchema({"type": "string"}, mode="serialization"),
]
