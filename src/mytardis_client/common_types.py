"""Definitions of fundamental data types used when interacting with MyTardis.

Note that the specifications of the MyTardis objects are not included here; see
objects.py for that."""

from enum import Enum
from typing import Literal

from pydantic import RootModel, field_serializer, field_validator

from src.utils.validators import is_hex

# The HTTP methods supported by MyTardis. Can be used to constrain the request interfaces
# to ensure that only methods that are supported by MyTardis are used.
HttpRequestMethod = Literal["GET", "POST"]


class DataClassification(Enum):
    """An enumerator for data classification.

    Gaps have been left deliberately in the enumeration to allow for intermediate
    classifications of data that may arise. The larger the integer that the classification
    resolves to, the less sensitive the data is.
    """

    RESTRICTED = 1
    SENSITIVE = 25
    INTERNAL = 50
    PUBLIC = 100


class DataStatus(Enum):
    """An enumerator for data status.

    Gaps have been left deliberately in the enumeration to allow for intermediate
    classifications of data that may arise.
    """

    READY_FOR_INGESTION = 1
    INGESTED = 5
    FAILED = 10


class MD5Sum(RootModel[str], frozen=True):
    """A string representing an MD5Sum, validated to have the correct format."""

    root: str

    def __str__(self) -> str:
        return self.root

    @field_validator("root", mode="after")
    @classmethod
    def validate_format(cls, value: str) -> str:
        """Check that the URI is well-formed"""
        if len(value) != 32:
            raise ValueError("MD5Sum must contain exactly 32 characters")
        if not is_hex(value):
            raise ValueError("MD5Sum must be a valid hexadecimal string")

        return value

    @field_serializer("root")
    def serialize(self, md5sum: str) -> str:
        """Serialize the MD5Sum as a simple string"""

        return md5sum
