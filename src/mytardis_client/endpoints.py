"""Information about MyTardis endpoints"""

from pydantic import BaseModel

from src.mytardis_client.types import MyTardisObjectType


class MyTardisEndpoint(BaseModel):
    """Properties of a MyTardis endpoint"""

    url_suffix: str


# Mapping from MyTardis object to the corresponding API endpoint. Assumes the
# object corresponds to a single endpoint, which may not always hold(?), but is ok for now.
_MYTARDIS_OBJECT_ENDPOINTS: dict[MyTardisObjectType, MyTardisEndpoint] = {
    MyTardisObjectType.PROJECT: MyTardisEndpoint(
        url_suffix="project",
    ),
    MyTardisObjectType.EXPERIMENT: MyTardisEndpoint(
        url_suffix="experiment",
    ),
    MyTardisObjectType.DATASET: MyTardisEndpoint(
        url_suffix="dataset",
    ),
    MyTardisObjectType.DATAFILE: MyTardisEndpoint(
        url_suffix="dataset_file",
    ),
    MyTardisObjectType.INSTITUTION: MyTardisEndpoint(
        url_suffix="institution",
    ),
    MyTardisObjectType.INSTRUMENT: MyTardisEndpoint(
        url_suffix="instrument",
    ),
    MyTardisObjectType.FACILITY: MyTardisEndpoint(
        url_suffix="facility",
    ),
    MyTardisObjectType.STORAGE_BOX: MyTardisEndpoint(
        url_suffix="storagebox",
    ),
    MyTardisObjectType.PROJECT_PARAMETER_SET: MyTardisEndpoint(
        url_suffix="projectparameterset",
    ),
    MyTardisObjectType.EXPERIMENT_PARAMETER_SET: MyTardisEndpoint(
        url_suffix="experimentparameterset"
    ),
    MyTardisObjectType.DATASET_PARAMETER_SET: MyTardisEndpoint(
        url_suffix="datasetparameterset",
    ),
}


def get_endpoint(object_type: MyTardisObjectType) -> MyTardisEndpoint:
    """Get the endpoint for a given MyTardis object type"""
    endpoint = _MYTARDIS_OBJECT_ENDPOINTS.get(object_type)

    if endpoint is None:
        raise ValueError(f"No known endpoint exists for object type {object_type}")

    return endpoint
