"""Definitions of fundamental data types used when interacting with MyTardis.

Note that the specifications of the MyTardis objects are not included here; see
objects.py for that."""

import re
from typing import Any, Literal
from urllib.parse import urlparse

from pydantic import RootModel, field_serializer, field_validator

from src.mytardis_client.objects import KNOWN_MYTARDIS_OBJECTS

HttpRequestMethod = Literal["GET", "POST", "PUT", "DELETE", "PATCH"]

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


def resource_uri_to_id(uri: str) -> int:
    """Gets the id from a resource URI

    Takes resource URI like: http://example.org/api/v1/experiment/998
    and returns just the id value (998).

    Args:
        uri: str - the URI from MyTardis

    Returns:
        The integer id that maps to the URI
    """
    uri_sep: str = "/"
    return int(urlparse(uri).path.rstrip(uri_sep).split(uri_sep).pop())


class URI(RootModel[str], frozen=True):
    """A MyTardis URI string, with validation and serialization logic."""

    root: str

    def __str__(self) -> str:
        return self.root

    @property
    def id(self) -> int:
        """Get the ID from the URI"""
        return resource_uri_to_id(self.root)

    @field_validator("root", mode="after")
    @classmethod
    def validate_uri(cls, value: str) -> str:
        """Check that the URI is well-formed"""
        return validate_uri(value)

    @field_serializer("root")
    def serialize_uri(self, uri: str) -> str:
        """Serialize the URI as a simple string"""

        return uri
