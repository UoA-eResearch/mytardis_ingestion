# pylint: disable=too-few-public-methods,no-name-in-module
""" Pydantic model to hold storage box from MyTardis"""

from enum import Enum


class StorageTypesEnum(Enum):
    """An enumerator to host the different storage types that can be used by
    MyTardis"""

    FILE_SYSTEM = "file_system"
    S3 = "s3"
