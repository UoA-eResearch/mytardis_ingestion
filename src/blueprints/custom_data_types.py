# pylint: disable=consider-using-f-string,unnecessary-lambda
""" This module defines custom data types for use in the MyTardis ingestion scripts.
 These data types include validators and custom exceptions for accurate logging and error
handling.
"""

import re
from typing import Annotated, Any

from pydantic import AfterValidator, PlainSerializer, WithJsonSchema
from validators import url

KNOWN_MYTARDIS_OBJECTS = [
    "datafileparameterset",
    "datafileparameter",
    "dataset",
    "dataset_file",
    "datasetparameter",
    "datasetparameterset",
    "experiment",
    "experimentparameter",
    "experimentparameterset",
    "facility",
    "group",
    "institution",
    "instrument",
    "parametername",
    "project",
    "projectparameter",
    "projectparameterset",
    "replica",
    "schema",
    "storagebox",
    "user",
]

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


def gen_uri_regex(  # pylint: disable=missing-function-docstring
    object_type: str,
) -> str:
    return f"^/api/v1/{object_type}/[0-9]{{1,}}/"


def validate_uri(value: Any) -> str:
    """Validator for a URI string to ensure that it matches the expected form of a URI"""
    if not isinstance(value, str):
        raise TypeError(f'Unexpected type for URI: "{type(value)}"')
    object_type = uri_regex.search(value.lower())
    if not object_type:
        raise ValueError(
            f'Passed string value "{value}" is not a well formatted MyTardis URI'
        )
    object_type_str = object_type.group(1)
    if object_type_str.lower() not in KNOWN_MYTARDIS_OBJECTS:
        raise ValueError(f'Unknown object type: "{object_type_str}"')
    return value


URI = Annotated[
    str,
    AfterValidator(validate_uri),
    PlainSerializer(lambda x: str(x), return_type=str),
    WithJsonSchema({"type": "string"}, mode="serialization"),
]


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


'''class BaseObjectType(str):
    """Class method that defines a validated string which includes the four object types
    found in MyTardis when projects are activated.This Type class is intended to be
    called by an Inner class defined in the Smelter/Crucible/Overseer and
    IngestionFactory classes to reduce the number of verifications of project activation
    needed since the "project" key value will fail validation unless projects are
    active."""

    BASE_OBJECTS = [
        "experiment",
        "dataset",
        "datafile",
        "project",
    ]

    @classmethod
    def __get_validators__(  # type: ignore[return]
        cls,
    ) -> Generator[Callable[[str], str], str, str]:
        """One or more validators may be yielded which will be called in order to validate the
        input. Each validator will receive as an input the value returned from the previous
        validator. (As per the Pydantic help manual).
        """
        yield cls.validate

    @classmethod
    def validate(cls, value: Any) -> str:
        """Custom validator to ensure that the string is one of the known objects in
        MyTardis."""
        if not isinstance(value, str):
            raise TypeError(f'Unexpected type for BaseObjectType "{type(value)}"')
        if value not in cls.BASE_OBJECTS:
            raise ValueError(
                'Passed string value "%s" is not a recognised MyTardis '
                "object." % (value)
            )
        return cls(f"{value}")

    def __repr__(self) -> str:
        """Indicate that it is a formatted BaseObjectType string via __repr__"""
        return f"BaseObjectType({super().__repr__()})"'''
