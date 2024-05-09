# pylint: disable=missing-class-docstring,fixme
"""Useful enunmerators for ingestion"""

from enum import Enum
from typing import TypedDict


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
