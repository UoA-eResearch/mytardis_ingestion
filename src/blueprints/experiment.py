# pylint: disable=too-few-public-methods,no-name-in-module
"""Pydantic model defining an Experiment for ingestion into MyTardis"""

from abc import ABC
from datetime import datetime
from typing import List, Optional, Union

from pydantic import BaseModel, HttpUrl

from src.blueprints.common_models import GroupACL, ParameterSet, UserACL
from src.blueprints.custom_data_types import URI, ISODateTime


class BaseExperiment(BaseModel, ABC):
    """Abstract base class for an experiment. The two concrete child classes
    validate against different standards, with the Experiment having a more strict
    validation than the RawExperiment class."""

    title: str
    description: str
    institution_name: Optional[str] = "The University of Auckland"
    created_by: Optional[str] = None
    url: Optional[HttpUrl] = None
    locked: Optional[bool] = False
    users: Optional[List[UserACL]] = None
    groups: Optional[List[GroupACL]] = None
    persistent_id: Optional[str] = None
    alternate_ids: Optional[List[str]] = None


class RawExperiment(BaseExperiment):
    """Concrete class for raw data as read in from the metadata file."""

    projects: Optional[List[str]] = None
    start_time: Optional[Union[datetime, str]] = None
    end_time: Optional[Union[datetime, str]] = None
    created_time: Optional[Union[datetime, str]] = None
    update_time: Optional[Union[datetime, str]] = None
    embargo_until: Optional[Union[datetime, str]] = None


class Experiment(BaseExperiment):
    """Concrete calss for the data as ready to ingest into MyTardis"""

    projects: Optional[List[URI]] = None
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
