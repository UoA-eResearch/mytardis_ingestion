# pylint: disable=too-few-public-methods,no-name-in-module,duplicate-code
""" Pydantic model defining a Project for ingestion into MyTardis."""

from abc import ABC
from datetime import datetime
from typing import Dict, List, Optional

from pydantic import BaseModel, Field

from src.blueprints.common_models import GroupACL, ParameterSet, UserACL
from src.blueprints.custom_data_types import ISODateTime, MTUrl, Username
from src.mytardis_client.endpoints.endpoints import URI
from src.mytardis_client.enumerators import DataClassification, DataStatus


class BaseProject(BaseModel, ABC):
    """Abstract base class for a project. The two concrete child classes
    validate against different standards, with the Project having a more strict
    validation than the RawProject class."""

    name: str = Field(min_length=1)
    description: str
    principal_investigator: Username
    data_classification: DataClassification = DataClassification.SENSITIVE
    data_status: Optional[DataStatus] = None
    created_by: Optional[str] = None
    url: Optional[str] = None
    users: Optional[List[UserACL]] = None
    groups: Optional[List[GroupACL]] = None
    identifiers: Optional[List[str]] = None

    @property
    def display_name(self) -> str:
        """Display name for a project"""
        return self.name


class RawProject(BaseProject):
    """Concrete class to hold data from the concrete smelter class
    with a validated format as an entry point into the smelting process."""

    institution: Optional[List[str]] = None
    metadata: Optional[Dict[str, str | int | float | bool]] = None
    object_schema: Optional[MTUrl] = Field(default=None, alias="schema")
    start_time: Optional[datetime | str] = None
    end_time: Optional[datetime | str] = None
    embargo_until: Optional[datetime | str] = None


class RefinedProject(BaseProject):
    """Concrete class for raw data as read in from the metadata file.
    The difference between the RawProject and the Project is that the RawProject
    model has more general string hinting."""

    institution: List[str]
    start_time: Optional[datetime | str] = None
    end_time: Optional[datetime | str] = None
    embargo_until: Optional[datetime | str] = None


class Project(BaseProject):
    """Concrete class for project data prepared for ingestion into MyTardis.
    The difference between the RawProject and the Project is that the RawProject
    model has more general string hinting."""

    institution: List[URI]
    start_time: Optional[ISODateTime] = None
    end_time: Optional[ISODateTime] = None
    embargo_until: Optional[ISODateTime] = None


class ProjectParameterSet(ParameterSet):
    """Subclass of the ParameterSet class to include the project associated with the
    metadata. Is defined as Optional as the metadata is generated into a ParameterSet by
    the smelter class and the URI is not known until the project has been forged."""

    project: URI
