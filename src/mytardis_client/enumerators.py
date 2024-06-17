# pylint: disable=missing-class-docstring,fixme
"""Useful enunmerators for ingestion"""

from enum import Enum


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
