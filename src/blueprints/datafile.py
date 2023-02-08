# pylint: disable=too-few-public-methods,no-name-in-module
"""Pydantic model defining a Dataset for ingestion into MyTardis"""

from pathlib import Path
from typing import Dict, List, Optional

from pydantic import AnyUrl, BaseModel, Field

from src.blueprints.common_models import GroupACL, ParameterSet, UserACL
from src.blueprints.custom_data_types import URI


class DatafileReplica(BaseModel):
    """Pydantic model to ensure there is a validated replica of the file
    for ingestion into MyTardis."""

    uri: str
    location: str
    protocol: str = "file"


class BaseDatafile(BaseModel):
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
            the size in bytes of the file to be ingested"""

    filename: str
    directory: Path
    md5sum: str
    mimetype: str
    size: int
    users: Optional[List[UserACL]] = None
    groups: Optional[List[GroupACL]] = None
    # persistent_id: Optional[str] = None
    # alternate_ids: Optional[List[str]] = None


class RawDatafile(BaseDatafile):
    """Concrete class to hold data from the concrete smelter class
    with a validated format as an entry point into the smelting process."""

    dataset: str
    metadata: Optional[Dict[str, str | int | float | bool]] = None
    object_schema: Optional[AnyUrl] = Field(default=None, alias="schema")


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
