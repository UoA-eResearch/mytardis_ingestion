# pylint: disable=too-few-public-methods,no-name-in-module
"""Pydantic model defining a Dataset for ingestion into MyTardis"""

from abc import ABC
from pathlib import Path
from typing import Dict, List, Optional

from pydantic import BaseModel, Field

from src.blueprints.common_models import GroupACL, ParameterSet, UserACL
from src.blueprints.custom_data_types import URI, MTUrl


class DatafileReplica(BaseModel):
    """Pydantic model to ensure there is a validated replica of the file
    for ingestion into MyTardis.

    Attributes:
        location (str): The storage box associated with the replica
    """

    uri: str
    location: str
    protocol: str = "file"


class BaseDatafile(BaseModel, ABC):
    """Abstract base class for a dataset. The two concrete child classes
    validate against different standards, with the Dataset having a more strict
    validation than the RawDataset class.

    Attributes:
        filename: str
            the name of the file to be ingested
        directory: Path
            the path to the directory containing the file, RELATIVE to the
            source directory
        md5sum: str
            the md5 checksum of the file to be ingested
        mimetype: str
            the MIME type of the file to be ingested
        size: int
            the size in bytes of the file to be ingested
        archive_date: ISODateTime
            the date that the datafile will be automatically archived
        delete_date: ISODateTime
            the date that the datafile is able to be deleted"""

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
    metadata: Optional[Dict[str, str | int | float | bool | None]] = None
    object_schema: Optional[MTUrl] = Field(default=None, alias="schema")


class RefinedDatafile(BaseDatafile):
    """Concrete class for raw data as read in from the metadata file."""

    dataset: str
    parameter_sets: Optional[ParameterSet] = None


class Datafile(BaseDatafile):
    """Concrete class for the data as ready to ingest into MyTardis"""

    replicas: List[DatafileReplica]
    parameter_sets: Optional[ParameterSet] = None
    dataset: URI
