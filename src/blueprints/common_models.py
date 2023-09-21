# pylint: disable=too-few-public-methods,no-name-in-module,unnecessary-lambda

"""Pydantic dataclasses that are used in multiple places."""

import re
from typing import Annotated, Any, List, Optional, Union

from pydantic import (
    AfterValidator,
    BaseModel,
    Field,
    PlainSerializer,
    ValidationError,
    WithJsonSchema,
)
from validators import url

from src.blueprints.custom_data_types import (
    KNOWN_MYTARDIS_OBJECTS,
    Username,
    validate_uri,
    validate_url,
)


class UserACL(BaseModel):
    """Pydantic model to define user access control. This differs from the group
    access control in that it validates the username against a known regex.
    """

    user: Username
    is_owner: bool = False
    can_download: bool = False
    see_sensitive: bool = False


class GroupACL(BaseModel):
    """Pydantic model to define group access control."""

    group: str
    is_owner: bool = False
    can_download: bool = False
    see_sensitive: bool = False


class Parameter(BaseModel):
    """Pydantic class to hold an individual parameter in an appropriate format
    for inclusion into a ParameterSet.
    """

    name: str
    value: Union[str, int, float, bool]

    def __lt__(self, other: "Parameter") -> bool:
        if self.name != other.name:
            return self.name < other.name
        if isinstance(self.value, (int, float)) and isinstance(
            other.value, (int, float)
        ):
            return self.value < other.value
        if isinstance(self.value, str) and isinstance(other.value, str):
            return self.value < other.value
        if isinstance(self.value, bool) and isinstance(other.value, bool):
            return self.value and other.value
        raise ValueError("Trying to compare different value types")

    def __gt__(self, other: "Parameter") -> bool:
        if self.name != other.name:
            return self.name > other.name
        if isinstance(self.value, (int, float)) and isinstance(
            other.value, (int, float)
        ):
            return self.value > other.value
        if isinstance(self.value, str) and isinstance(other.value, str):
            return self.value > other.value
        if isinstance(self.value, bool) and isinstance(other.value, bool):
            return self.value and other.value
        raise ValueError("Trying to compare different value types")

    def __le__(self, other: "Parameter") -> bool:
        if self.name != other.name:
            return self.name <= other.name
        if isinstance(self.value, (int, float)) and isinstance(
            other.value, (int, float)
        ):
            return self.value <= other.value
        if isinstance(self.value, str) and isinstance(other.value, str):
            return self.value <= other.value
        if isinstance(self.value, bool) and isinstance(other.value, bool):
            return self.value and other.value
        raise ValueError("Trying to compare different value types")

    def __ge__(self, other: "Parameter") -> bool:
        if self.name != other.name:
            return self.name >= other.name
        if isinstance(self.value, (int, float)) and isinstance(
            other.value, (int, float)
        ):
            return self.value >= other.value
        if isinstance(self.value, str) and isinstance(other.value, str):
            return self.value >= other.value
        if isinstance(self.value, bool) and isinstance(other.value, bool):
            return self.value and other.value
        raise ValueError("Trying to compare different value types")


def validate_schema(value: Any) -> str:
    """Validator for schema, acts as a wrapper around the schemas for both URI and MTUrl

    Args:
        value (any): object to be tested

    Returns:
        str: validated string
    """
    if not isinstance(value, str):
        raise TypeError(f'Unexpected type for schema field: "{type(value)}"')
    uri_regex = re.compile(r"^/api/v1/([a-z]{1,}|dataset_file)/[0-9]{1,}/$")
    valid_flag = False
    object_type = uri_regex.search(value.lower())
    if object_type and object_type[1].lower() in KNOWN_MYTARDIS_OBJECTS:
        valid_flag = True
    if url(value):
        valid_flag = True
    if not valid_flag:
        raise ValidationError(f'Passed string "{value}" is not a valid URI or URL')
    return value


class ParameterSet(BaseModel):
    """Pydantic class to hold a parameter set ready for ingestion into MyTardis."""

    parameter_schema: Annotated[
        str,
        AfterValidator(validate_schema),
        PlainSerializer(lambda x: str(x), return_type=str),
        WithJsonSchema({"type": "string"}, mode="serialization"),
    ] = Field(alias="schema")
    parameters: Optional[List[Parameter]] = None
