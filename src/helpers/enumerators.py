"""Useful enunmerators for ingestion"""

from enum import Enum

from src.helpers.config import MyTardisObject


class ObjectPostEnum(Enum):
    """An enumverator for the POST/PUT/PATCH specific
    information used by MyTardis."""

    PROJECT = {
        "url_substring": "project",
        "expect_json": True,
    }
    EXPERIMENT = {
        "url_substring": "experiment",
        "expect_json": True,
    }
    DATASET = {
        "url_substring": "dataset",
        "expect_json": True,
    }
    DATAFILE = {
        "url_substring": "dataset_file",
        "expect_json": True,
    }
    PROJECT_PARAMETERS = {
        "url_substring": "projectparameterset",
        "expect_json": False,
    }
    EXPERIMENT_PARAMETERS = {
        "url_substring": "experimentparameterset",
        "expect_json": False,
    }
    DATASET_PARAMETERS = {
        "url_substring": "datsetparameterset",
        "expect_json": False,
    }


class ObjectSearchEnum(Enum):
    """An enumerator for objects that can be searched for in
    MyTardis via the API"""

    PROJECT = {
        "type": "project",
        "target": "name",
        "url_substring": "project",
    }
    EXPERIMENT = {
        "type": "experiment",
        "target": "title",
        "url_substring": "experiment",
    }
    DATASET = {
        "type": "dataset",
        "target": "description",
        "url_substring": "dataset",
    }
    DATAFILE = {
        "type": "datafile",
        "target": "filename",
        "url_substring": "dataset_file",
    }
    INSTITUTION = {
        "type": "institution",
        "target": "name",
        "url_substring": "institution",
    }
    INSTRUMENT = {
        "type": "instrument",
        "target": "name",
        "url_substring": "instrument",
    }
    FACILITY = {
        "type": "facility",
        "target": "name",
        "url_substring": "facility",
    }
    STORAGE_BOX = {
        "type": "storagebox",
        "target": "name",
        "url_substring": "storagebox",
    }


class ObjectEnum(Enum):
    """An enumerator for hierarchy objects in MyTardis"""

    PROJECT = {
        "type": MyTardisObject.project,
        "name": "name",
        "match_keys": [
            "name",
            "description",
            "principal_investigator",
        ],
        "parent": None,
        "search_type": ObjectSearchEnum.PROJECT,
    }
    EXPERIMENT = {
        "type": MyTardisObject.experiment,
        "name": "title",
        "match_keys": [
            "title",
            "description",
        ],
        "parent": "project",
        "search_type": ObjectSearchEnum.EXPERIMENT,
    }
    DATASET = {
        "type": MyTardisObject.dataset,
        "name": "description",
        "match_keys": [
            "description",
        ],
        "parent": "experiment",
        "search_type": ObjectSearchEnum.DATASET,
    }
    DATAFILE = {
        "type": MyTardisObject.datafile,
        "name": "filename",
        "match_keys": [
            "filename",
            "size",
            "md5sum",
        ],
        "parent": "dataset",
        "search_type": ObjectSearchEnum.DATAFILE,
    }
