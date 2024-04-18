# pylint: disable=missing-class-docstring,fixme
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
    STORAGE_BOX = "storagebox"
    USER = "user"


class URLSubstring(str, Enum):
    """Enum to convert between human readable and URL substrings"""

    PROJECT = "project"
    EXPERIMENT = "experiment"
    DATASET = "dataset"
    DATAFILE = "dataset_file"
    INSTITUTION = "institution"
    INSTRUMENT = "instrument"
    FACILITY = "facility"
    STORAGE_BOX = "storagebox"
    PROJECT_PARAMETERS = "projectparameterset"
    EXPERIMENT_PARAMETERS = "experimentparameterset"
    DATASET_PARAMETERS = "datasetparameterset"


class ObjectPostDict(TypedDict):
    """An object to account for whether or not a return is expected from the API call."""

    url_substring: URLSubstring
    expect_json: bool


class ObjectPostEnum(Enum):
    """An enumerator for the POST/PUT/PATCH specific
    information used by MyTardis."""

    PROJECT: ObjectPostDict = {
        "url_substring": URLSubstring.PROJECT,
        "expect_json": True,
    }
    EXPERIMENT: ObjectPostDict = {
        "url_substring": URLSubstring.EXPERIMENT,
        "expect_json": True,
    }
    DATASET: ObjectPostDict = {
        "url_substring": URLSubstring.DATASET,
        "expect_json": True,
    }
    DATAFILE: ObjectPostDict = {
        "url_substring": URLSubstring.DATAFILE,
        "expect_json": False,
    }
    PROJECT_PARAMETERS: ObjectPostDict = {
        "url_substring": URLSubstring.PROJECT_PARAMETERS,
        "expect_json": False,
    }
    EXPERIMENT_PARAMETERS: ObjectPostDict = {
        "url_substring": URLSubstring.EXPERIMENT_PARAMETERS,
        "expect_json": False,
    }
    DATASET_PARAMETERS: ObjectPostDict = {
        "url_substring": URLSubstring.DATASET_PARAMETERS,
        "expect_json": False,
    }


class ObjectSearchDict(TypedDict):
    type: MyTardisObject
    target: str
    url_substring: URLSubstring


# FIXME there is quite a substantial problem with how we search for potential
# matches; whatever we search on must be unique, e.g. 2 files have the same
# name, but are part of different datasets with our current matching logic the 2
# files will be partial matches and therefore the new file won't be ingested.
# This is not a problem now as we have removed the functionality to match
# existing objects in MyTardis, but it might be relevant in the future.
class ObjectSearchEnum(Enum):
    """An enumerator for objects that can be searched for in
    MyTardis via the API"""

    PROJECT: ObjectSearchDict = {
        "type": MyTardisObject.PROJECT,
        "target": "name",
        "url_substring": URLSubstring.PROJECT,
    }
    EXPERIMENT: ObjectSearchDict = {
        "type": MyTardisObject.EXPERIMENT,
        "target": "title",
        "url_substring": URLSubstring.EXPERIMENT,
    }
    DATASET: ObjectSearchDict = {
        "type": MyTardisObject.DATASET,
        "target": "description",
        "url_substring": URLSubstring.DATASET,
    }
    DATAFILE: ObjectSearchDict = {
        "type": MyTardisObject.DATAFILE,
        "target": "filename",
        "url_substring": URLSubstring.DATAFILE,
    }
    INSTITUTION: ObjectSearchDict = {
        "type": MyTardisObject.INSTITUTION,
        "target": "name",
        "url_substring": URLSubstring.INSTITUTION,
    }
    INSTRUMENT: ObjectSearchDict = {
        "type": MyTardisObject.INSTRUMENT,
        "target": "name",
        "url_substring": URLSubstring.INSTRUMENT,
    }
    FACILITY: ObjectSearchDict = {
        "type": MyTardisObject.FACILITY,
        "target": "name",
        "url_substring": URLSubstring.FACILITY,
    }
    STORAGE_BOX: ObjectSearchDict = {
        "type": MyTardisObject.STORAGE_BOX,
        "target": "name",
        "url_substring": URLSubstring.STORAGE_BOX,
    }


class ObjectDict(TypedDict):
    type: MyTardisObject
    name: str
    match_keys: list[str]
    parent: MyTardisObject | None
    search_type: ObjectSearchDict


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
        "search_type": ObjectSearchEnum.PROJECT.value,
    }
    EXPERIMENT: ObjectDict = {
        "type": MyTardisObject.EXPERIMENT,
        "name": "title",
        "match_keys": [
            "title",
            "description",
        ],
        "parent": MyTardisObject.PROJECT,
        "search_type": ObjectSearchEnum.EXPERIMENT.value,
    }
    DATASET: ObjectDict = {
        "type": MyTardisObject.DATASET,
        "name": "description",
        "match_keys": [
            "description",
        ],
        "parent": MyTardisObject.EXPERIMENT,
        "search_type": ObjectSearchEnum.DATASET.value,
    }
    DATAFILE: ObjectDict = {
        "type": MyTardisObject.DATAFILE,
        "name": "filename",
        "match_keys": [
            "filename",
            "size",
            "md5sum",
        ],
        "parent": MyTardisObject.DATASET,
        "search_type": ObjectSearchEnum.DATAFILE.value,
    }


class DataClassification(Enum):
    """An enumerator for data classification.

    Gaps have been left deliberately in the enumeration to allow for intermediate
    classifications of data that may arise. The larger the integer that the classification
    resolves to, the less sensitive the data is.
    """

    RESTRICTED = 1
    SENSITIVE = 25
    INTERNAL = 50
    PUBLIC = 100


class DataStatus(Enum):
    """An enumerator for data status.

    Gaps have been left deliberately in the enumeration to allow for intermediate
    classifications of data that may arise.
    """

    READY_FOR_INGESTION = 1
    INGESTED = 5
    FAILED = 10
