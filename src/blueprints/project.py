# pylint: disable=too-few-public-methods,no-name-in-module
""" Pydantic model defining a Project for ingestion into MyTardis."""

from abc import ABC
from datetime import datetime
from typing import Dict, List, Optional

from pydantic import AnyUrl, BaseModel, Field, HttpUrl

from src.blueprints.common_models import GroupACL, ParameterSet, UserACL
from src.blueprints.custom_data_types import URI, ISODateTime, Username


class BaseProject(BaseModel, ABC):
    """Abstract base class for a project. The two concrete child classes
    validate against different standards, with the Project having a more strict
    validation than the RawProject class."""

    name: str
    description: str
    principal_investigator: Username
    created_by: Optional[str] = None
    url: Optional[HttpUrl] = None
    users: Optional[List[UserACL]] = None
    groups: Optional[List[GroupACL]] = None
    persistent_id: Optional[str] = None
    alternate_ids: Optional[List[str]] = None


class RawProject(BaseProject):
    """Concrete class to hold data from the concrete smelter class
    with a validated format as an entry point into the smelting process."""

    instituiton: Optional[List[str]]
    metadata: Optional[List[Dict[str, str | int | float | bool]]]
    project_schema: Optional[AnyUrl] = None
    start_time: Optional[datetime | str] = None
    end_time: Optional[datetime | str] = None
    embargo_until: Optional[datetime | str] = None

    class Config:
        """alias the project_scheam field as schema"""

        fields = {"project_schema": "schema"}


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
