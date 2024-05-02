"""Type information for MyTardis objects"""

from enum import Enum

from pydantic import BaseModel


# NOTE: we should move away from using strings as the values for the enum,
#       as this encourages "stringly-typed" interfaces. Too much change for now.
class MyTardisObjectType(Enum):
    """Enum for possible MyTardis object types"""

    PROJECT = 0
    EXPERIMENT = 1
    DATASET = 2
    DATAFILE = 3
    INSTRUMENT = 4
    INSTITUTION = 5
    FACILITY = 6
    STORAGE_BOX = 7
    USER = 8
    PROJECT_PARAMETER_SET = 9
    EXPERIMENT_PARAMETER_SET = 10
    DATASET_PARAMETER_SET = 11


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
