# pylint: disable=consider-using-f-string,unnecessary-lambda
""" This module defines custom data types for use in the MyTardis ingestion scripts.
 These data types include validators and custom exceptions for accurate logging and error
handling.
"""

import re
from typing import Annotated, Any

from pydantic import AfterValidator, PlainSerializer, WithJsonSchema
from validators import url

from src.mytardis_client.objects import KNOWN_MYTARDIS_OBJECTS

user_regex = re.compile(
    r"^[a-z]{2,4}[0-9]{3}$"  # Define as a constant in case of future change
)
uri_regex = re.compile(r"^/api/v1/([a-z]{1,}|dataset_file)/[0-9]{1,}/$")
iso_time_regex = re.compile(
    r"^(-?(?:[1-9][0-9]*)?[0-9]{4})-(1[0-2]|0[1-9])-(3[01]|0[1-9]|[12][0-9])T(2[0-3]|[01][0-9]):([0-5][0-9]):([0-5][0-9])(\.[0-9]+)?(Z|[+-](?:2[0-3]|[01][0-9]):[0-5][0-9])?$"  # pylint: disable=line-too-long
)
iso_date_regex = re.compile(
    r"^(-?(?:[1-9][0-9]*)?[0-9]{4})-(1[0-2]|0[1-9])-(3[01]|0[1-9]|[12][0-9])"
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


ISODateTime = Annotated[
    str,
    AfterValidator(validate_isodatetime),
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


def validate_schema(value: Any) -> str:
    """Validator for schema, acts as a wrapper around the schemas for both URI and MTUrl

    Args:
        value (any): object to be tested

    Returns:
        str: validated string
    """
    if not isinstance(value, str):
        raise TypeError(f'Unexpected type for schema field: "{type(value)}"')
    valid_flag = False
    object_type = uri_regex.match(value.lower())
    if object_type and object_type[1].lower() in KNOWN_MYTARDIS_OBJECTS:
        valid_flag = True
    elif url(value):
        valid_flag = True
    if not valid_flag:
        raise ValueError(f'Passed string "{value}" is not a valid URI or URL')
    return value
