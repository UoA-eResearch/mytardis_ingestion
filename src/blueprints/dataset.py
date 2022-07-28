# pylint: disable=too-few-public-methods,no-name-in-module
"""Pydantic model defining a Dataset for ingestion into MyTardis"""

from abc import ABC
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Union

from pydantic import BaseModel

from src.blueprints.common_models import GroupACL, ParameterSet, UserACL
from src.blueprints.custom_data_types import URI, ISODateTime


class BaseDataset(BaseModel, ABC):
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
    """Concrete class for raw data as read in from the metadata file."""

    experiments: List[str]
    instrument: str
    created_time: Optional[Union[datetime, str]] = None
    modified_time: Optional[Union[datetime, str]] = None


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
