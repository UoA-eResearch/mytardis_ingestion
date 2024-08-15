"""Definitions of fundamental data types used when interacting with MyTardis.

Note that the specifications of the MyTardis objects are not included here; see
objects.py for that."""

from enum import Enum
from typing import Annotated, Literal

from pydantic import AfterValidator

from src.utils.validation import validate_isodatetime, validate_md5sum, validate_url

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


MD5Sum = Annotated[
    str,
    AfterValidator(validate_md5sum),
]

ISODateTime = Annotated[
    str,
    AfterValidator(validate_isodatetime),
]

MTUrl = Annotated[
    str,
    AfterValidator(validate_url),
]
