# pylint: disable=too-few-public-methods,no-name-in-module,duplicate-code
""" Pydantic model defining a Project for ingestion into MyTardis."""

from abc import ABC
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from pydantic import BaseModel, Field

from src.blueprints.common_models import GroupACL, ParameterSet, UserACL
from src.blueprints.custom_data_types import URI, ISODateTime, MTUrl, Username
from src.helpers.enumerators import DataClassification


class ProjectStorageBox(BaseModel, ABC):
    """Abstract base class for a project storage box. Concrete child classes
    will need to be constructed for each new Django storage types

    Attributes:
        name (str): the storage box name
        storage_class (str): The Django storage class
        options (dict): A dictionary of options for the storage type that are project specific
        delete_in_days (int): duration after which time the data can be flagged for deletion
        archive_in_days (int): duration after which the data can be removed from active
            stores
    """

    name: Optional[str] = None
    storage_class: str
    options: Optional[Dict[str, str]] = None
    delete_in_days: int
    archive_in_days: int


class ProjectFileSystemStorageBox(ProjectStorageBox):
    """Concrete sub-class of the ProjectStorageBox for unix filesystems"""

    target_directory: Path


class ProjectS3StorageBox(ProjectStorageBox):
    """Concrete sub-class of the ProjectStorageBox for S3 buckets"""

    bucket: str
    target_key: str


class BaseProject(BaseModel, ABC):
    """Abstract base class for a project. The two concrete child classes
    validate against different standards, with the Project having a more strict
    validation than the RawProject class."""

    name: str
    description: str
    principal_investigator: Username
    data_classification: DataClassification = DataClassification.SENSITIVE
    created_by: Optional[str] = None
    url: Optional[str] = None
    users: Optional[List[UserACL]] = None
    groups: Optional[List[GroupACL]] = None
    identifiers: Optional[List[str]] = None


class RawProject(BaseProject):
    """Concrete class to hold data from the concrete smelter class
    with a validated format as an entry point into the smelting process."""

    institution: Optional[List[str]] = None
    metadata: Optional[Dict[str, str | int | float | bool]] = None
    object_schema: Optional[MTUrl] = Field(default=None, alias="schema")
    start_time: Optional[datetime | str] = None
    end_time: Optional[datetime | str] = None
    embargo_until: Optional[datetime | str] = None
    active_stores: Optional[List[ProjectStorageBox]] = None
    archives: Optional[List[ProjectStorageBox]] = None
    delete_in_days: Optional[int]
    archive_in_days: Optional[int]


class RefinedProject(BaseProject):
    """Concrete class for raw data as read in from the metadata file.
    The difference between the RawProject and the Project is that the RawProject
    model has more general string hinting."""

    institution: List[str]
    start_time: Optional[datetime | str] = None
    end_time: Optional[datetime | str] = None
    embargo_until: Optional[datetime | str] = None
    active_stores: List[ProjectStorageBox]
    archives: List[ProjectStorageBox]


class Project(BaseProject):
    """Concrete class for project data prepared for ingestion into MyTardis.
    The difference between the RawProject and the Project is that the RawProject
    model has more general string hinting."""

    institution: List[URI]
    start_time: Optional[ISODateTime] = None
    end_time: Optional[ISODateTime] = None
    embargo_until: Optional[ISODateTime] = None
    active_stores: List[ProjectStorageBox]
    archives: List[ProjectStorageBox]


class ProjectParameterSet(ParameterSet):
    """Subclass of the ParameterSet class to include the project associated with the
    metadata. Is defined as Optional as the metadata is generated into a ParameterSet by
    the smelter class and the URI is not known until the project has been forged."""

    project: URI
