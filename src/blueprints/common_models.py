# pylint: disable=too-few-public-methods,no-name-in-module

"""Pydantic dataclasses that are used in multiple places."""

from typing import List, Optional, Union

from pydantic import BaseModel, Field

from src.blueprints.custom_data_types import URI, Username


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


class ParameterSet(BaseModel):
    """Pydantic class to hold a parameter set ready for ingestion into MyTardis."""

    parameter_schema: URI = Field(alias="schema")
    parameters: Optional[List[Parameter]] = None
