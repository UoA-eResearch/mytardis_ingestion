"""Information about MyTardis endpoints"""

import re
from typing import Any, Literal, Optional
from urllib.parse import urlparse

from pydantic import BaseModel, RootModel, field_serializer, field_validator

from src.mytardis_client.objects import MyTardisObject, list_mytardis_objects

MyTardisEndpoint = Literal[
    "/dataset",
    "/datasetparameterset",
    "/dataset_file",
    "/experiment",
    "/experimentparameterset",
    "/facility",
    "/group",
    "/institution",
    "/instrument",
    "/introspection",
    "/project",
    "/projectparameterset",
    "/schema",
    "/storagebox",
    "/user",
]


class GetRequestProperties(BaseModel):
    """Definition of behaviour/structure for a GET request to a MyTardis endpoint."""

    # Note: it would be useful here to store the dataclass type for the response, so that
    #       the response can be validated/deserialized without the requester needing
    #       to know the correct type. But the dataclasses are currently defined outside the
    #       mytardis_client module, and this module should ideally be self-contained.
    response_obj_type: MyTardisObject


class PostRequestProperties(BaseModel):
    """Definition of behaviour/structure for a POST request to a MyTardis endpoint."""

    expect_response_json: bool
    # response_obj_type: MyTardisObject
    request_body_obj_type: MyTardisObject


class EndpointMethods(BaseModel):
    """Definition of the methods available for an endpoint, and their properties."""

    GET: Optional[GetRequestProperties] = None
    POST: Optional[PostRequestProperties] = None


class MyTardisEndpointInfo(BaseModel):
    """Properties of a MyTardis endpoint"""

    path: MyTardisEndpoint
    methods: EndpointMethods


# Information about each MyTardis endpoint
_MYTARDIS_ENDPOINTS: dict[MyTardisEndpoint, MyTardisEndpointInfo] = {
    "/project": MyTardisEndpointInfo(
        path="/project",
        methods=EndpointMethods(
            GET=GetRequestProperties(
                response_obj_type=MyTardisObject.PROJECT,
            ),
            POST=PostRequestProperties(
                expect_response_json=True,
                request_body_obj_type=MyTardisObject.PROJECT,
            ),
        ),
    ),
    "/experiment": MyTardisEndpointInfo(
        path="/experiment",
        methods=EndpointMethods(
            GET=GetRequestProperties(
                response_obj_type=MyTardisObject.EXPERIMENT,
            ),
            POST=PostRequestProperties(
                expect_response_json=True,
                request_body_obj_type=MyTardisObject.EXPERIMENT,
            ),
        ),
    ),
    "/dataset": MyTardisEndpointInfo(
        path="/dataset",
        methods=EndpointMethods(
            GET=GetRequestProperties(
                response_obj_type=MyTardisObject.DATASET,
            ),
            POST=PostRequestProperties(
                expect_response_json=True,
                request_body_obj_type=MyTardisObject.DATASET,
            ),
        ),
    ),
    "/dataset_file": MyTardisEndpointInfo(
        path="/dataset_file",
        methods=EndpointMethods(
            GET=GetRequestProperties(
                response_obj_type=MyTardisObject.DATAFILE,
            ),
            POST=PostRequestProperties(
                expect_response_json=False,
                request_body_obj_type=MyTardisObject.DATAFILE,
            ),
        ),
    ),
    "/institution": MyTardisEndpointInfo(
        path="/institution",
        methods=EndpointMethods(
            GET=GetRequestProperties(
                response_obj_type=MyTardisObject.INSTITUTION,
            ),
        ),
    ),
    "/instrument": MyTardisEndpointInfo(
        path="/instrument",
        methods=EndpointMethods(
            GET=GetRequestProperties(
                response_obj_type=MyTardisObject.INSTRUMENT,
            ),
        ),
    ),
    "/facility": MyTardisEndpointInfo(
        path="/facility",
        methods=EndpointMethods(
            GET=GetRequestProperties(
                response_obj_type=MyTardisObject.FACILITY,
            ),
        ),
    ),
    "/storagebox": MyTardisEndpointInfo(
        path="/storagebox",
        methods=EndpointMethods(
            GET=GetRequestProperties(
                response_obj_type=MyTardisObject.STORAGE_BOX,
            ),
        ),
    ),
    "/projectparameterset": MyTardisEndpointInfo(
        path="/projectparameterset",
        methods=EndpointMethods(
            GET=GetRequestProperties(
                response_obj_type=MyTardisObject.PROJECT_PARAMETER_SET,
            ),
            POST=PostRequestProperties(
                expect_response_json=False,
                request_body_obj_type=MyTardisObject.PROJECT_PARAMETER_SET,
            ),
        ),
    ),
    "/experimentparameterset": MyTardisEndpointInfo(
        path="/experimentparameterset",
        methods=EndpointMethods(
            GET=GetRequestProperties(
                response_obj_type=MyTardisObject.EXPERIMENT_PARAMETER_SET,
            ),
            POST=PostRequestProperties(
                expect_response_json=False,
                request_body_obj_type=MyTardisObject.EXPERIMENT_PARAMETER_SET,
            ),
        ),
    ),
    "/datasetparameterset": MyTardisEndpointInfo(
        path="/datasetparameterset",
        methods=EndpointMethods(
            GET=GetRequestProperties(
                response_obj_type=MyTardisObject.DATASET_PARAMETER_SET,
            ),
            POST=PostRequestProperties(
                expect_response_json=False,
                request_body_obj_type=MyTardisObject.DATASET_PARAMETER_SET,
            ),
        ),
    ),
    "/introspection": MyTardisEndpointInfo(
        path="/introspection",
        methods=EndpointMethods(
            GET=GetRequestProperties(
                response_obj_type=MyTardisObject.INTROSPECTION,
            ),
        ),
    ),
}


def get_endpoint_info(endpoint: MyTardisEndpoint) -> MyTardisEndpointInfo:
    """Get the endpoint for a given MyTardis object type"""
    endpoint_info = _MYTARDIS_ENDPOINTS.get(endpoint)

    if endpoint_info is None:
        raise ValueError(f"{endpoint_info} is not a known MyTardis endpoint")

    return endpoint_info


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
    if object_type_str.lower() not in list_mytardis_objects():
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
    """A URI string identifying a MyTardis object.

    Expected to be of the form: /api/v1/<endpoint>/<id>/
    """

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
