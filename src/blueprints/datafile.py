# pylint: disable=too-few-public-methods,no-name-in-module
"""Pydantic model defining a Dataset for ingestion into MyTardis"""

from abc import ABC
from pathlib import Path
from typing import Dict, List, Optional

from pydantic import AnyUrl, BaseModel

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
    directory: Path
    md5sum: str
    mimetype: str
    size: int
    users: Optional[List[UserACL]] = None
    groups: Optional[List[GroupACL]] = None


class RawDatafile(BaseDatafile):
    """Concrete class to hold data from the concrete smelter class
    with a validated format as an entry point into the smelting process."""

    dataset: str
    metadata: Optional[List[Dict[str, str | int | float | bool]]]
    datafile_schema: Optional[AnyUrl] = None

    class Config:
        """alias the datafile_scheam field as schema"""

        fields = {"datafile_schema": "schema"}


class RefinedDatafile(BaseDatafile):
    """Concrete class for raw data as read in from the metadata file."""

    dataset: str
    replicas: List[DatafileReplica]
    parameter_sets: Optional[ParameterSet] = None


class Datafile(BaseDatafile):
    """Concrete class for the data as ready to ingest into MyTardis"""

    replicas: List[DatafileReplica]
    parameter_sets: Optional[ParameterSet] = None
    dataset: URI
