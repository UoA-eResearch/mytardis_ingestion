# pylint: disable=too-few-public-methods,no-name-in-module,duplicate-code
"""Pydantic model defining a Dataset for ingestion into MyTardis"""

from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from pydantic import AnyUrl, BaseModel, Field

from src.blueprints.common_models import GroupACL, ParameterSet, UserACL
from src.blueprints.custom_data_types import URI, ISODateTime


class BaseDataset(BaseModel):
    """Abstract base class for a dataset. The two concrete child classes
    validate against different standards, with the Dataset having a more strict
    validation than the RawDataset class."""

    description: str
    directory: Optional[Path] = None
    users: Optional[List[UserACL]] = None
    groups: Optional[List[GroupACL]] = None
    immutable: bool = False
    persistent_id: Optional[str] = None
    alternate_ids: Optional[List[str]] = None


class RawDataset(BaseDataset):
    """Concrete class to hold data from the concrete smelter class
    with a validated format as an entry point into the smelting process."""

    experiments: List[str]
    instrument: str
    metadata: Optional[Dict[str, str | int | float | bool]] = None
    object_schema: Optional[AnyUrl] = Field(default=None, alias="schema")
    created_time: Optional[datetime | str] = None
    modified_time: Optional[datetime | str] = None


class RefinedDataset(BaseDataset):
    """Concrete class for raw data as read in from the metadata file."""

    experiments: List[str]
    instrument: str
    created_time: Optional[datetime | str] = None
    modified_time: Optional[datetime | str] = None


class Dataset(BaseDataset):
    """Concrete class for the data as ready to ingest into MyTardis"""

    experiments: List[URI]
    instrument: URI
    created_time: Optional[ISODateTime] = None
    modified_time: Optional[ISODateTime] = None


class DatasetParameterSet(ParameterSet):
    """Subclass of the ParameterSet class to include the dataset associated with the
    metadata. Is defined as Optional as the metadata is generated into a ParameterSet by
    the smelter class and the URI is not known until the experiment has been forged."""

    dataset: URI
