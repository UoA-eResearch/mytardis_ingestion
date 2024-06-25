"""Information about MyTardis endpoints"""

from typing import Literal, Optional

from pydantic import BaseModel

from src.mytardis_client.objects import MyTardisObject

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
