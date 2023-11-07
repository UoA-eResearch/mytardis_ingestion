# pylint: disable=too-few-public-methods,no-name-in-module
""" Pydantic model to hold storage box from MyTardis"""

from pathlib import Path
from typing import Dict, Optional

from pydantic import BaseModel, ValidationInfo, field_validator

from src.blueprints.custom_data_types import URI


class RawStorageBox(BaseModel):
    """A storage box class to hold the name, location, uri and description
    of a MyTardis storage box object"""

    name: str
    storage_class: str = "django.core.files.storage.FileSystemStorage"
    description: str = ""
    options: Dict[str, str]
    attributes: Optional[Dict[str, str]] = None

    @field_validator("description")
    @classmethod
    def get_description(  # pylint: disable=E0213
        cls,
        description: str,
        info: ValidationInfo,
    ) -> str:
        """Generate a default description if one is not provided."""
        if not description and "name" in info.data:
            return f'Storage box for {info.data["name"]}'
        return description


class StorageBox(BaseModel):
    """A storage box class to hold the name, location, uri and description
    of a MyTardis storage box object"""

    name: str
    description: str
    uri: URI
    location: Path
