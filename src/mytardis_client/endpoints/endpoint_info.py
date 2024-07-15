"""Information about MyTardis endpoints, including the expected request/response types."""

from typing import Optional, Type

from pydantic import BaseModel

from src.mytardis_client import mt_dataclasses as dc
from src.mytardis_client.endpoints.endpoints import MyTardisEndpoint
from src.mytardis_client.objects import MyTardisObject


class GetRequestProperties(BaseModel):
    """Definition of behaviour/structure for a GET request to a MyTardis endpoint."""

    # Note: it would be useful here to store the dataclass type for the response, so that
    #       the response can be validated/deserialized without the requester needing
    #       to know the correct type. But the dataclasses are currently defined outside the
    #       mytardis_client module, and this module should ideally be self-contained.
    response_obj_type: MyTardisObject
    response_dataclass: Type[dc.MyTardisResource]


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
_MYTARDIS_ENDPOINT_INFO: dict[MyTardisEndpoint, MyTardisEndpointInfo] = {
    "/project": MyTardisEndpointInfo(
        path="/project",
        methods=EndpointMethods(
            GET=GetRequestProperties(
                response_obj_type=MyTardisObject.PROJECT,
                response_dataclass=dc.IngestedProject,
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
                response_dataclass=dc.IngestedExperiment,
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
                response_dataclass=dc.IngestedDataset,
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
                response_dataclass=dc.IngestedDatafile,
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
                response_dataclass=dc.Institution,
            ),
        ),
    ),
    "/instrument": MyTardisEndpointInfo(
        path="/instrument",
        methods=EndpointMethods(
            GET=GetRequestProperties(
                response_obj_type=MyTardisObject.INSTRUMENT,
                response_dataclass=dc.Instrument,
            ),
        ),
    ),
    "/facility": MyTardisEndpointInfo(
        path="/facility",
        methods=EndpointMethods(
            GET=GetRequestProperties(
                response_obj_type=MyTardisObject.FACILITY,
                response_dataclass=dc.Facility,
            ),
        ),
    ),
    "/storagebox": MyTardisEndpointInfo(
        path="/storagebox",
        methods=EndpointMethods(
            GET=GetRequestProperties(
                response_obj_type=MyTardisObject.STORAGE_BOX,
                response_dataclass=dc.StorageBox,
            ),
        ),
    ),
    "/projectparameterset": MyTardisEndpointInfo(
        path="/projectparameterset",
        methods=EndpointMethods(
            GET=GetRequestProperties(
                response_obj_type=MyTardisObject.PROJECT_PARAMETER_SET,
                response_dataclass=dc.ProjectParameterSet,
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
                response_dataclass=dc.ExperimentParameterSet,
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
                response_dataclass=dc.DatasetParameterSet,
            ),
            POST=PostRequestProperties(
                expect_response_json=False,
                request_body_obj_type=MyTardisObject.DATASET_PARAMETER_SET,
            ),
        ),
    ),
    "/datafileparameterset": MyTardisEndpointInfo(
        path="/datafileparameterset",
        methods=EndpointMethods(
            GET=GetRequestProperties(
                response_obj_type=MyTardisObject.DATAFILE_PARAMETER_SET,
                response_dataclass=dc.DatafileParameterSet,
            ),
            POST=PostRequestProperties(
                expect_response_json=False,
                request_body_obj_type=MyTardisObject.DATAFILE_PARAMETER_SET,
            ),
        ),
    ),
    "/introspection": MyTardisEndpointInfo(
        path="/introspection",
        methods=EndpointMethods(
            GET=GetRequestProperties(
                response_obj_type=MyTardisObject.INTROSPECTION,
                response_dataclass=dc.MyTardisIntrospection,
            ),
        ),
    ),
}


def get_endpoint_info(endpoint: MyTardisEndpoint) -> MyTardisEndpointInfo:
    """Get the endpoint for a given MyTardis object type"""
    endpoint_info = _MYTARDIS_ENDPOINT_INFO.get(endpoint)

    if endpoint_info is None:
        raise ValueError(f"{endpoint_info} is not a known MyTardis endpoint")

    return endpoint_info
