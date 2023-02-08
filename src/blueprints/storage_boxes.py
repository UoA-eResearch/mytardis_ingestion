# pylint: disable=too-few-public-methods,no-name-in-module
""" Pydantic model to hold storage box from MyTardis"""

from pathlib import Path

from pydantic import BaseModel

from src.blueprints.custom_data_types import URI


class StorageBox(BaseModel):
    """A storage box class to hold the name, location, uri and description
    of a MyTardis storage box object"""

    name: str
    description: str
    uri: URI
    location: Path
