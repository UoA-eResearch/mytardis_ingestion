"""Information about MyTardis endpoints"""

from enum import Enum
from typing import Optional

from pydantic import BaseModel

from src.mytardis_client.objects import MyTardisObject


class MyTardisEndpoint(Enum):
    """
    Definition of the MyTardis object types.

    The underlying string values are intended to exactly match the names of the
    MyTardis objects, as used by the MyTardis API in its responses.
    (Note that this may differ from the endpoint names used in the API URLs.)

    The enum type should be used wherever possible, with the string values only
    used at the boundaries of the application, when communicating with MyTardis.
    """

    PROJECT = 0
    EXPERIMENT = 1
    DATASET = 2
    DATAFILE = 3
    INSTRUMENT = 4
    INSTITUTION = 5
    FACILITY = 6
    STORAGE_BOX = 7
    PROJECT_PARAMETER_SET = 8
    EXPERIMENT_PARAMETER_SET = 9
    DATASET_PARAMETER_SET = 10
    INTROSPECTION = 11


class GetRequestProperties(BaseModel):
    """Definition of behaviour/structure for a GET request to a MyTardis endpoint."""

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

    path: str
    methods: EndpointMethods


# Mapping from MyTardis object to the corresponding API endpoint. Assumes the
# object corresponds to a single endpoint, which may not always hold(?), but is ok for now.
_MYTARDIS_ENDPOINTS: dict[MyTardisEndpoint, MyTardisEndpointInfo] = {
    MyTardisEndpoint.PROJECT: MyTardisEndpointInfo(
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
    MyTardisEndpoint.EXPERIMENT: MyTardisEndpointInfo(
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
    MyTardisEndpoint.DATASET: MyTardisEndpointInfo(
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
    MyTardisEndpoint.DATAFILE: MyTardisEndpointInfo(
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
    MyTardisEndpoint.INSTITUTION: MyTardisEndpointInfo(
        path="/institution",
        methods=EndpointMethods(
            GET=GetRequestProperties(
                response_obj_type=MyTardisObject.INSTITUTION,
            ),
        ),
    ),
    MyTardisEndpoint.INSTRUMENT: MyTardisEndpointInfo(
        path="/instrument",
        methods=EndpointMethods(
            GET=GetRequestProperties(
                response_obj_type=MyTardisObject.INSTRUMENT,
            ),
        ),
    ),
    MyTardisEndpoint.FACILITY: MyTardisEndpointInfo(
        path="/facility",
        methods=EndpointMethods(
            GET=GetRequestProperties(
                response_obj_type=MyTardisObject.FACILITY,
            ),
        ),
    ),
    MyTardisEndpoint.STORAGE_BOX: MyTardisEndpointInfo(
        path="/storagebox",
        methods=EndpointMethods(
            GET=GetRequestProperties(
                response_obj_type=MyTardisObject.STORAGE_BOX,
            ),
        ),
    ),
    MyTardisEndpoint.PROJECT_PARAMETER_SET: MyTardisEndpointInfo(
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
    MyTardisEndpoint.EXPERIMENT_PARAMETER_SET: MyTardisEndpointInfo(
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
    MyTardisEndpoint.DATASET_PARAMETER_SET: MyTardisEndpointInfo(
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
    MyTardisEndpoint.INTROSPECTION: MyTardisEndpointInfo(
        path="/introspection",
        methods=EndpointMethods(
            GET=GetRequestProperties(
                response_obj_type=MyTardisObject.INTROSPECTION,
            ),
        ),
    ),
}


def get_endpoint_info(object_type: MyTardisEndpoint) -> MyTardisEndpointInfo:
    """Get the endpoint for a given MyTardis object type"""
    endpoint = _MYTARDIS_ENDPOINTS.get(object_type)

    if endpoint is None:
        raise ValueError(f"No known endpoint exists for object type {object_type}")

    return endpoint
