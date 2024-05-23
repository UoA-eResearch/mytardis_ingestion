# pylint: disable=too-few-public-methods,no-name-in-module
""" Pydantic model to hold storage box from MyTardis"""

from enum import Enum
from pathlib import Path

from pydantic import BaseModel

from src.mytardis_client.data_types import URI


class StorageTypesEnum(Enum):
    """An enumerator to host the different storage types that can be used by
    MyTardis"""

    FILE_SYSTEM = "file_system"
    S3 = "s3"


class StorageBox(BaseModel):
    """A storage box class to hold the name, location, uri and description
    of a MyTardis storage box object"""

    name: str
    description: str
    uri: URI
    location: Path
