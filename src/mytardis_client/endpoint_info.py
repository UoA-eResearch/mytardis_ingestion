"""Definitions of the properties of MyTardis endpoints"""

from typing import Optional

from pydantic import BaseModel

from src.mytardis_client.endpoints import MyTardisEndpoint
from src.mytardis_client.objects import MyTardisObject
from src.mytardis_client.response_data import (
    DatasetParameterSet,
    ExperimentParameterSet,
    Facility,
    IngestedDatafile,
    IngestedDataset,
    IngestedExperiment,
    IngestedProject,
    Institution,
    Instrument,
    MyTardisIntrospection,
    MyTardisObjectData,
    ProjectParameterSet,
    StorageBox,
)


class GetRequestProperties(BaseModel):
    """Definition of behaviour/structure for a GET request to a MyTardis endpoint."""

    response_obj_type: type[MyTardisObjectData]


class PostRequestProperties(BaseModel):
    """Definition of behaviour/structure for a POST request to a MyTardis endpoint."""

    expect_response_json: bool
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
                response_obj_type=IngestedProject,
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
                response_obj_type=IngestedExperiment,
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
                response_obj_type=IngestedDataset,
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
                response_obj_type=IngestedDatafile,
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
                response_obj_type=Institution,
            ),
        ),
    ),
    "/instrument": MyTardisEndpointInfo(
        path="/instrument",
        methods=EndpointMethods(
            GET=GetRequestProperties(
                response_obj_type=Instrument,
            ),
        ),
    ),
    "/facility": MyTardisEndpointInfo(
        path="/facility",
        methods=EndpointMethods(
            GET=GetRequestProperties(
                response_obj_type=Facility,
            ),
        ),
    ),
    "/storagebox": MyTardisEndpointInfo(
        path="/storagebox",
        methods=EndpointMethods(
            GET=GetRequestProperties(
                response_obj_type=StorageBox,
            ),
        ),
    ),
    "/projectparameterset": MyTardisEndpointInfo(
        path="/projectparameterset",
        methods=EndpointMethods(
            GET=GetRequestProperties(
                response_obj_type=ProjectParameterSet,
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
                response_obj_type=ExperimentParameterSet,
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
                response_obj_type=DatasetParameterSet,
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
                response_obj_type=MyTardisIntrospection,
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
