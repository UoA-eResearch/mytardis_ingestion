# pylint: disable=missing-class-docstring
"""Useful enunmerators for ingestion"""

from enum import Enum
from typing import TypedDict


class MyTardisObject(str, Enum):
    """Enum for possible MyTardis object types"""

    DATASET = "dataset"
    EXPERIMENT = "experiment"
    FACILITY = "facility"
    INSTRUMENT = "instrument"
    PROJECT = "project"
    INSTITUTION = "institution"
    DATAFILE = "datafile"


class ObjectPostDict(TypedDict):
    url_substring: str
    expect_json: bool


class ObjectPostEnum(Enum):
    """An enumverator for the POST/PUT/PATCH specific
    information used by MyTardis."""

    PROJECT: ObjectPostDict = {
        "url_substring": "project",
        "expect_json": True,
    }
    EXPERIMENT: ObjectPostDict = {
        "url_substring": "experiment",
        "expect_json": True,
    }
    DATASET: ObjectPostDict = {
        "url_substring": "dataset",
        "expect_json": True,
    }
    DATAFILE: ObjectPostDict = {
        "url_substring": "dataset_file",
        "expect_json": True,
    }
    PROJECT_PARAMETERS: ObjectPostDict = {
        "url_substring": "projectparameterset",
        "expect_json": False,
    }
    EXPERIMENT_PARAMETERS: ObjectPostDict = {
        "url_substring": "experimentparameterset",
        "expect_json": False,
    }
    DATASET_PARAMETERS: ObjectPostDict = {
        "url_substring": "datsetparameterset",
        "expect_json": False,
    }


class ObjectSearchDict(TypedDict):
    type: str
    target: str
    url_substring: str


# FIXME there is quite a substantial problem with how we search for potential
# matches; whatever we search on must be unique, e.g. 2 files have the same
# name, but are part of different datasets with our current matching logic the 2
# files will be partial matches and therefore the new file won't be ingested
class ObjectSearchEnum(Enum):
    """An enumerator for objects that can be searched for in
    MyTardis via the API"""

    PROJECT: ObjectSearchDict = {
        "type": "project",
        "target": "name",
        "url_substring": "project",
    }
    EXPERIMENT: ObjectSearchDict = {
        "type": "experiment",
        "target": "title",
        "url_substring": "experiment",
    }
    DATASET: ObjectSearchDict = {
        "type": "dataset",
        "target": "description",
        "url_substring": "dataset",
    }
    DATAFILE: ObjectSearchDict = {
        "type": "datafile",
        "target": "filename",
        "url_substring": "dataset_file",
    }
    INSTITUTION: ObjectSearchDict = {
        "type": "institution",
        "target": "name",
        "url_substring": "institution",
    }
    INSTRUMENT: ObjectSearchDict = {
        "type": "instrument",
        "target": "name",
        "url_substring": "instrument",
    }
    FACILITY: ObjectSearchDict = {
        "type": "facility",
        "target": "name",
        "url_substring": "facility",
    }
    STORAGE_BOX: ObjectSearchDict = {
        "type": "storagebox",
        "target": "name",
        "url_substring": "storagebox",
    }


class ObjectDict(TypedDict):
    type: MyTardisObject
    name: str
    match_keys: list[str]
    parent: str | None
    search_type: ObjectSearchEnum


# TODO do we need more match_keys like pid?
class ObjectEnum(Enum):
    """An enumerator for hierarchy objects in MyTardis"""

    PROJECT: ObjectDict = {
        "type": MyTardisObject.PROJECT,
        "name": "name",
        "match_keys": [
            "name",
            "description",
            "principal_investigator",
        ],
        "parent": None,
        "search_type": ObjectSearchEnum.PROJECT,
    }
    EXPERIMENT: ObjectDict = {
        "type": MyTardisObject.EXPERIMENT,
        "name": "title",
        "match_keys": [
            "title",
            "description",
        ],
        "parent": "project",
        "search_type": ObjectSearchEnum.EXPERIMENT,
    }
    DATASET: ObjectDict = {
        "type": MyTardisObject.DATASET,
        "name": "description",
        "match_keys": [
            "description",
        ],
        "parent": "experiment",
        "search_type": ObjectSearchEnum.DATASET,
    }
    DATAFILE: ObjectDict = {
        "type": MyTardisObject.DATAFILE,
        "name": "filename",
        "match_keys": [
            "filename",
            "size",
            "md5sum",
        ],
        "parent": "dataset",
        "search_type": ObjectSearchEnum.DATAFILE,
    }
