# pylint: disable=too-few-public-methods,no-name-in-module
"""Pydantic model defining a Dataset for ingestion into MyTardis"""

from abc import ABC
from pathlib import Path
from typing import Dict, List, Optional

from pydantic import BaseModel, Field, field_serializer

from src.blueprints.common_models import GroupACL, ParameterSet, UserACL
from src.mytardis_client.common_types import DataStatus, MTUrl
from src.mytardis_client.endpoints import URI


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
    """

    filename: str = Field(min_length=1)
    directory: Optional[Path] = None
    md5sum: str
    mimetype: str
    size: int
    users: Optional[List[UserACL]] = None
    groups: Optional[List[GroupACL]] = None
    data_status: Optional[DataStatus] = None

    @property
    def display_name(self) -> str:
        """Display name for a datafile (not necessarily unique)"""
        return self.filename

    @property
    def filepath(self) -> Path:
        """The full path to the file"""
        if self.directory is not None:
            return self.directory / self.filename
        return Path(self.filename)

    @field_serializer("directory")
    def dir_as_posix_path(self, directory: Optional[Path]) -> Optional[str]:
        """Ensures the directory is always serialized as a posix path, or `None` if not set."""
        return directory.as_posix() if directory else None


class RawDatafile(BaseDatafile):
    """Concrete class to hold data from the concrete smelter class
    with a validated format as an entry point into the smelting process."""

    dataset: str
    metadata: Optional[Dict[str, str | int | float | bool]] = None
    object_schema: Optional[MTUrl] = Field(default=None, alias="schema")


class RefinedDatafile(BaseDatafile):
    """Concrete class for raw data as read in from the metadata file."""

    dataset: str
    parameter_sets: Optional[List[ParameterSet]] = None


class Datafile(BaseDatafile):
    """Concrete class for the data as ready to ingest into MyTardis"""

    replicas: List[DatafileReplica]
    parameter_sets: Optional[List[ParameterSet]] = None
    dataset: URI
