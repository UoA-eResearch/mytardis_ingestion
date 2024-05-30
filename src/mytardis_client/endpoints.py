"""Information about MyTardis endpoints"""

from pydantic import BaseModel

from src.mytardis_client.objects import MyTardisObject


class MyTardisEndpoint(BaseModel):
    """Properties of a MyTardis endpoint"""

    url_suffix: str


# Mapping from MyTardis object to the corresponding API endpoint. Assumes the
# object corresponds to a single endpoint, which may not always hold(?), but is ok for now.
_MYTARDIS_OBJECT_ENDPOINTS: dict[MyTardisObject, MyTardisEndpoint] = {
    MyTardisObject.PROJECT: MyTardisEndpoint(
        url_suffix="project",
    ),
    MyTardisObject.EXPERIMENT: MyTardisEndpoint(
        url_suffix="experiment",
    ),
    MyTardisObject.DATASET: MyTardisEndpoint(
        url_suffix="dataset",
    ),
    MyTardisObject.DATAFILE: MyTardisEndpoint(
        url_suffix="dataset_file",
    ),
    MyTardisObject.INSTITUTION: MyTardisEndpoint(
        url_suffix="institution",
    ),
    MyTardisObject.INSTRUMENT: MyTardisEndpoint(
        url_suffix="instrument",
    ),
    MyTardisObject.FACILITY: MyTardisEndpoint(
        url_suffix="facility",
    ),
    MyTardisObject.STORAGE_BOX: MyTardisEndpoint(
        url_suffix="storagebox",
    ),
    MyTardisObject.PROJECT_PARAMETER_SET: MyTardisEndpoint(
        url_suffix="projectparameterset",
    ),
    MyTardisObject.EXPERIMENT_PARAMETER_SET: MyTardisEndpoint(
        url_suffix="experimentparameterset"
    ),
    MyTardisObject.DATASET_PARAMETER_SET: MyTardisEndpoint(
        url_suffix="datasetparameterset",
    ),
}


def get_endpoint(object_type: MyTardisObject) -> MyTardisEndpoint:
    """Get the endpoint for a given MyTardis object type"""
    endpoint = _MYTARDIS_OBJECT_ENDPOINTS.get(object_type)

    if endpoint is None:
        raise ValueError(f"No known endpoint exists for object type {object_type}")

    return endpoint
