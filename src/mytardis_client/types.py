"""Type information for MyTardis objects"""

from enum import Enum

from pydantic import BaseModel


class MyTardisObjectType(str, Enum):
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


class MyTardisTypeInfo(BaseModel):
    """Properties of a MyTardis object type"""

    match_fields: list[str]


_MYTARDIS_TYPE_INFO: dict[MyTardisObjectType, MyTardisTypeInfo] = {
    MyTardisObjectType.PROJECT: MyTardisTypeInfo(
        match_fields=[
            "name",
        ],
    ),
    MyTardisObjectType.EXPERIMENT: MyTardisTypeInfo(
        match_fields=[
            "title",
        ],
    ),
    MyTardisObjectType.DATASET: MyTardisTypeInfo(
        match_fields=[
            "description",
            "instrument",
        ],
    ),
    MyTardisObjectType.DATAFILE: MyTardisTypeInfo(
        match_fields=[
            "filename",
            "directory",
            "dataset",
        ],
    ),
    MyTardisObjectType.INSTRUMENT: MyTardisTypeInfo(
        match_fields=[
            "name",
        ],
    ),
    MyTardisObjectType.INSTITUTION: MyTardisTypeInfo(
        match_fields=[
            "name",
        ],
    ),
    MyTardisObjectType.FACILITY: MyTardisTypeInfo(
        match_fields=[
            "name",
        ],
    ),
    MyTardisObjectType.STORAGE_BOX: MyTardisTypeInfo(
        match_fields=[
            "name",
        ],
    ),
    MyTardisObjectType.USER: MyTardisTypeInfo(
        match_fields=[
            "username",
        ],
    ),
}


def get_type_info(object_type: MyTardisObjectType) -> MyTardisTypeInfo:
    """Get the type info for a given MyTardis object type"""
    type_info = _MYTARDIS_TYPE_INFO.get(object_type)

    if type_info is None:
        raise ValueError(f"No type info defined for object type {object_type}")

    return type_info
