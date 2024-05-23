"""Definitions of fundamental data types used when interacting with MyTardis.

Note that the specifications of the MyTardis objects are not included here; see
objects.py for that."""

import re
from typing import Any
from urllib.parse import urlparse

from pydantic import RootModel, SerializationInfo, field_serializer, field_validator

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


_KEY_USE_ID_ONLY_FROM_URI = "use_only_id_from_uri"
CONTEXT_USE_URI_ID_ONLY = {_KEY_USE_ID_ONLY_FROM_URI: True}


class URI(RootModel[str], frozen=True):
    """A MyTardis URI string, with validation and serialization logic."""

    root: str

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
    def serialize_uri(self, uri: str, info: SerializationInfo) -> str:
        """Serialize a URI, optionally extracting just the ID component, as MyTardis
        requires this sometimes (e.g. for GET requests), but we only know this at
        serialization time.

        The context can be passed to a Pydantic model containing URI fields, and it will be
        propagated down into all URI fields, so the serialization can be controlled in
        one place. Ideally the MyTardis client would encapsulate this logic eventually.
        """

        if context := info.context:
            if context.get(_KEY_USE_ID_ONLY_FROM_URI):
                return str(resource_uri_to_id(uri))

        return uri
