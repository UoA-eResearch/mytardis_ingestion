"""Information about MyTardis endpoints"""

import re
from typing import Any, Literal, get_args
from urllib.parse import urlparse

from pydantic import RootModel, field_serializer, field_validator

MyTardisEndpoint = Literal[
    "/datafileparameter",
    "/datafileparameterset",
    "/dataset",
    "/datasetparameter",
    "/datasetparameterset",
    "/dataset_file",
    "/experiment",
    "/experimentparameter",
    "/experimentparameterset",
    "/facility",
    "/group",
    "/institution",
    "/instrument",
    "/introspection",
    "/parametername",
    "/project",
    "/projectparameter",
    "/projectparameterset",
    "/replica",
    "/schema",
    "/storagebox",
    "/user",
]

_MYTARDIS_ENDPOINTS = list(get_args(MyTardisEndpoint))


def list_mytardis_endpoints() -> list[str]:
    """List the names of all MyTardis endpoints"""
    return _MYTARDIS_ENDPOINTS


uri_regex = re.compile(r"^/api/v1/([a-z_]{1,})/[0-9]{1,}/$")


def validate_uri(value: Any) -> str:
    """Validator for a URI string to ensure that it matches the expected form of a URI"""
    if not isinstance(value, str):
        raise TypeError(f'Unexpected type for URI: "{type(value)}"')
    endpoint = uri_regex.search(value.lower())
    if not endpoint:
        raise ValueError(
            f'Passed string value "{value}" is not a well formatted MyTardis URI'
        )
    endpoint_str = endpoint.group(1)

    candidate_endpoint = f"/{endpoint_str.lower()}"
    if candidate_endpoint not in _MYTARDIS_ENDPOINTS:
        raise ValueError(f'Unknown endpoint: "{endpoint_str}"')
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
    """A URI string identifying a MyTardis object.

    Expected to be of the form: /api/v1/<endpoint>/<id>/
    """

    root: str

    def __lt__(self, other: "URI") -> bool:
        return self.root < other.root

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
