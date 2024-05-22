# pylint: disable=unnecessary-lambda

"""Definitions of fundamental data types used when interacting with MyTardis.

Note that the specifications of the MyTardis objects are not included here; see
objects.py for that."""

import re
from typing import Annotated, Any

from pydantic import AfterValidator, PlainSerializer, WithJsonSchema

from src.mytardis_client.objects import KNOWN_MYTARDIS_OBJECTS

uri_regex = re.compile(r"^/api/v1/([a-z]{1,}|dataset_file)/[0-9]{1,}/$")


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
