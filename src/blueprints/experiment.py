# pylint: disable=too-few-public-methods,no-name-in-module
"""Pydantic model defining an Experiment for ingestion into MyTardis"""

from abc import ABC
from datetime import datetime
from typing import Dict, List, Optional

from pydantic import AnyUrl, BaseModel, HttpUrl

from src.blueprints.common_models import GroupACL, ParameterSet, UserACL
from src.blueprints.custom_data_types import URI, ISODateTime


class BaseExperiment(BaseModel, ABC):
    """Abstract base class for an experiment. The two concrete child classes
    validate against different standards, with the Experiment having a more strict
    validation than the RawExperiment class."""

    title: str
    description: str
    created_by: Optional[str] = None
    url: Optional[HttpUrl] = None
    locked: bool = False
    users: Optional[List[UserACL]] = None
    groups: Optional[List[GroupACL]] = None
    persistent_id: Optional[str] = None
    alternate_ids: Optional[List[str]] = None


class RawExperiment(BaseExperiment):
    """Concrete class to hold data from the concrete smelter class
    with a validated format as an entry point into the smelting process."""

    projects: Optional[List[str]] = None
    institution_name: Optional[str]
    metadata: Optional[List[Dict[str, str | int | float | bool]]]
    experiment_schema: Optional[AnyUrl] = None
    start_time: Optional[datetime | str] = None
    end_time: Optional[datetime | str] = None
    created_time: Optional[datetime | str] = None
    update_time: Optional[datetime | str] = None
    embargo_until: Optional[datetime | str] = None

    class Config:
        """alias the experiment_scheam field as schema"""

        fields = {"experiment_schema": "schema"}


class RefinedExperiment(BaseExperiment):
    """Concrete class for raw data as read in from the metadata file."""

    projects: Optional[List[str]] = None
    institution_name: str
    start_time: Optional[datetime | str] = None
    end_time: Optional[datetime | str] = None
    created_time: Optional[datetime | str] = None
    update_time: Optional[datetime | str] = None
    embargo_until: Optional[datetime | str] = None


class Experiment(BaseExperiment):
    """Concrete calss for the data as ready to ingest into MyTardis"""

    projects: Optional[List[URI]] = None
    institution_name: str
    start_time: Optional[ISODateTime] = None
    end_time: Optional[ISODateTime] = None
    created_time: Optional[ISODateTime] = None
    update_time: Optional[ISODateTime] = None
    embargo_until: Optional[ISODateTime] = None


class ExperimentParameterSet(ParameterSet):
    """Subclass of the ParameterSet class to include the experiment associated with the
    metadata. Is defined as Optional as the metadata is generated into a ParameterSet by
    the smelter class and the URI is not known until the experiment has been forged."""

    experiment: URI
