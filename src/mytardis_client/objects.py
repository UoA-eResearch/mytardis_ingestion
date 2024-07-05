"""Type information for MyTardis objects"""

from enum import Enum

from pydantic import BaseModel

from src.mytardis_client.data_types import URI
from src.mytardis_client.enumerators import DataClassification


class MyTardisObject(str, Enum):
    """
    Definition of the MyTardis object types.

    The underlying string values are intended to exactly match the names of the
    MyTardis objects, as used by the MyTardis API in its responses.
    (Note that this may differ from the endpoint names used in the API URLs.)

    The enum type should be used wherever possible, with the string values only
    used at the boundaries of the application, when communicating with MyTardis.
    """

    DATAFILE = "datafile"
    DATAFILE_PARAMETER = "datafileparameter"
    DATAFILE_PARAMETER_SET = "datafileparameterset"
    DATASET = "dataset"
    DATASET_PARAMETER = "datasetparameter"
    DATASET_PARAMETER_SET = "datasetparameterset"
    EXPERIMENT = "experiment"
    EXPERIMENT_PARAMETER = "experimentparameter"
    EXPERIMENT_PARAMETER_SET = "experimentparameterset"
    FACILITY = "facility"
    GROUP = "group"
    INSTITUTION = "institution"
    INSTRUMENT = "instrument"
    INTROSPECTION = "introspection"
    PARAMETER_NAME = "parametername"
    PROJECT = "project"
    PROJECT_PARAMETER = "projectparameter"
    PROJECT_PARAMETER_SET = "projectparameterset"
    REPLICA = "replica"
    SCHEMA = "schema"
    STORAGE_BOX = "storagebox"
    USER = "user"


_MYTARDIS_OBJECTS = [e.value for e in MyTardisObject]


def list_mytardis_objects() -> list[str]:
    """List the names of all MyTardis objects"""
    return _MYTARDIS_OBJECTS


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


# from __future__ import annotations

from typing import Any


class IngestedProject(BaseModel):
    classification: DataClassification
    created_by: URI
    # datafile_count: int
    # dataset_count: int
    description: str
    embargo_until: Any
    end_time: Any
    # experiment_count: int
    id: int
    identifiers: list[str]
    institution: list[str]
    locked: bool
    name: str
    parameter_sets: list
    principal_investigator: str
    public_access: int
    resource_uri: str
    size: int
    start_time: str
    tags: Any
    url: Any
