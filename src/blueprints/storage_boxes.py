# pylint: disable=too-few-public-methods,no-name-in-module
""" Pydantic model to hold storage box from MyTardis"""

from pathlib import Path
from typing import Any, Dict, Optional

from pydantic import BaseModel, field_validator

from src.blueprints.custom_data_types import URI


class RawStorageBox(BaseModel):
    """A storage box class to hold the name, location, uri and description
    of a MyTardis storage box object"""

    name: str
    storage_class: str = "django.core.files.storage.FileSystemStorage"
    description: str = ""
    options: Dict[str, str]
    attributes: Optional[Dict[str, str]] = None

    # TODO[pydantic]: We couldn't refactor the `validator`, please replace it by `field_validator`
    # manually.
    # Check https://docs.pydantic.dev/dev-v2/migration/#changes-to-validators for more information.
    @field_validator("description")
    def get_description(  # pylint: disable=E0213
        cls,
        description: str,
        values: Dict[str, Any],
    ) -> str:
        """Generate a default description if one is not provided."""
        if not description and "name" in values:
            return f'Storage box for {values["name"]}'
        return description


class StorageBox(BaseModel):
    """A storage box class to hold the name, location, uri and description
    of a MyTardis storage box object"""

    name: str
    description: str
    uri: URI
    location: Path
