"""Type information for MyTardis objects"""

from enum import Enum

from pydantic import BaseModel

# NOTE: moved from blueprints module, as it's thematically part of the MyTardis client.
#       Should try to unify with MyTardisObject below though.
KNOWN_MYTARDIS_OBJECTS = [
    "datafileparameterset",
    "datafileparameter",
    "dataset",
    "dataset_file",
    "datasetparameter",
    "datasetparameterset",
    "experiment",
    "experimentparameter",
    "experimentparameterset",
    "facility",
    "group",
    "institution",
    "instrument",
    "parametername",
    "project",
    "projectparameter",
    "projectparameterset",
    "replica",
    "schema",
    "storagebox",
    "user",
]


class MyTardisObject(str, Enum):
    """
    Definition of the MyTardis object types.

    The underlying string values are intended to exactly match the names of the
    MyTardis objects, as used by the MyTardis API in its responses.
    (Note that this may differ from the endpoint names used in the API URLs.)

    The enum type should be used wherever possible, with the string values only
    used at the boundaries of the application, when communicating with MyTardis.
    """

    PROJECT = "project"
    EXPERIMENT = "experiment"
    DATASET = "dataset"
    DATAFILE = "datafile"
    INSTRUMENT = "instrument"
    INSTITUTION = "institution"
    FACILITY = "facility"
    STORAGE_BOX = "storagebox"
    USER = "user"
    PROJECT_PARAMETER_SET = "projectparameterset"
    EXPERIMENT_PARAMETER_SET = "experimentparameterset"
    DATASET_PARAMETER_SET = "datasetparameterset"
    INTROSPECTION = "introspection"  # TEMPORARY: needed for endpoint defs, but we should use dataclass types there


class MyTardisTypeInfo(BaseModel):
    """Properties of a MyTardis object type"""

    # Note that the match field here is type-specific. Some types may also support
    # general identifier-based matching.
    match_fields: list[str]


_MYTARDIS_TYPE_INFO: dict[MyTardisObject, MyTardisTypeInfo] = {
    MyTardisObject.PROJECT: MyTardisTypeInfo(
        match_fields=[
            "name",
        ],
    ),
    MyTardisObject.EXPERIMENT: MyTardisTypeInfo(
        match_fields=[
            "title",
        ],
    ),
    MyTardisObject.DATASET: MyTardisTypeInfo(
        match_fields=[
            "description",
            "experiments",
            "instrument",
        ],
    ),
    MyTardisObject.DATAFILE: MyTardisTypeInfo(
        match_fields=[
            "filename",
            "directory",
            "dataset",
        ],
    ),
    MyTardisObject.INSTRUMENT: MyTardisTypeInfo(
        match_fields=[
            "name",
        ],
    ),
    MyTardisObject.INSTITUTION: MyTardisTypeInfo(
        match_fields=[
            "name",
        ],
    ),
    MyTardisObject.FACILITY: MyTardisTypeInfo(
        match_fields=[
            "name",
        ],
    ),
    MyTardisObject.STORAGE_BOX: MyTardisTypeInfo(
        match_fields=[
            "name",
        ],
    ),
    MyTardisObject.USER: MyTardisTypeInfo(
        match_fields=[
            "username",
        ],
    ),
}


def get_type_info(object_type: MyTardisObject) -> MyTardisTypeInfo:
    """Get the type info for a given MyTardis object type"""
    type_info = _MYTARDIS_TYPE_INFO.get(object_type)

    if type_info is None:
        raise ValueError(f"No type info defined for object type {object_type}")

    return type_info
