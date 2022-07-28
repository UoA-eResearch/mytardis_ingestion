# pylint: disable=too-few-public-methods,no-name-in-module
"""Pydantic model defining a Dataset for ingestion into MyTardis"""

from abc import ABC
from pathlib import Path
from typing import List, Optional

from pydantic import BaseModel

from src.blueprints.common_models import GroupACL, ParameterSet, UserACL
from src.blueprints.custom_data_types import URI


class DatafileReplica(BaseModel):
    """Pydantic model to ensure there is a validated replica of the file
    for ingestion into MyTardis."""

    uri: str
    location: str
    protocol: str = "file"


class BaseDatafile(BaseModel, ABC):
    """Abstract base class for a dataset. The two concrete child classes
    validate against different standards, with the Dataset having a more strict
    validation than the RawDataset class."""

    filename: str
    md5sum: str
    mimetype: str
    size: int
    replicas: List[DatafileReplica]
    parameter_sets: Optional[ParameterSet] = None
    directory: Optional[Path] = None
    users: Optional[List[UserACL]] = None
    groups: Optional[List[GroupACL]] = None


class RawDatafile(BaseDatafile):
    """Concrete class for raw data as read in from the metadata file."""

    dataset: str


class Datafile(BaseDatafile):
    """Concrete class for the data as ready to ingest into MyTardis"""

    dataset: URI
