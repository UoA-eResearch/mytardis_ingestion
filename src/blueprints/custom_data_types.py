# pylint: disable=consider-using-f-string
""" This module defines custom data types for use in the MyTardis ingestion scripts.
 These data types include validators and custom exceptions for accurate logging and error
handling.
"""

import re
from datetime import datetime

KNOWN_MYTARDIS_OBJECTS = [
    "datafileparameterset",
    "datafileparameter",
    "dataset",
    "dataset_file",
    "datasetparameter",
    "datasetparameterset",
    "experiment",
    "experimentparameter",
    "experimentparameterset",
    "facility",
    "group",
    "institution",
    "instrument",
    "parametername",
    "project",
    "projectparameter",
    "projectparameterset",
    "replica",
    "schema",
    "storagebox",
    "user",
]

user_regex = re.compile(
    r"^[a-z]{2,4}[0-9]{3}$"  # Define as a constant incase of future change
)
uri_regex = re.compile(r"^/api/v1/([a-z]{1,}|dataset_file)/[0-9]{1,}/$")
iso_time_regex = re.compile(
    r"^(-?(?:[1-9][0-9]*)?[0-9]{4})-(1[0-2]|0[1-9])-(3[01]|0[1-9]|[12][0-9])T(2[0-3]|[01][0-9]):([0-5][0-9]):([0-5][0-9])(\.[0-9]+)?(Z|[+-](?:2[0-3]|[01][0-9]):[0-5][0-9])?$"  # pylint: disable=line-too-long
)


def gen_uri_regex(object_type):  # pylint: disable=missing-function-docstring
    return f"^/api/v1/{object_type}/[0-9]{{1,}}/"


class Username(str):
    """Defines a validated username, in other words, ensure that the username meets a standardised
    format appropriate to the institution.

    Note this is a user class defined for the Universiy of Auckland UPI format. For
    other username formats please update the user_regex pattern.
    """

    @classmethod
    def __get_validators__(cls):
        """One or more validators may be yieled which will be called in order to validate the
        input. Each validator will receive as an input the value returned from the previous
        validator. (As per the Pydantic help manual).
        """
        yield cls.validate

    @classmethod
    def validate(cls, value):
        """Custom validator to ensure that the value is a string object and that it matches
        the regex defined for users"""
        if not isinstance(value, str):
            raise TypeError('Unexpected type for Username: "%s"' % type(value))
        match = user_regex.fullmatch(value.lower())
        if not match:
            raise ValueError(
                'Passed string value "%s" is not a well formatted ' "Username" % (value)
            )
        return cls(f"{match.group(0)}")

    def __repr__(self):
        """Indicate that the username object is a username"""
        return f"Username({super().__repr__()})"


class URI(str):
    """Defines a MyTardis URI as a subclass of the string class"""

    @classmethod
    def __get_validators__(cls):
        """One or more validators may be yieled which will be called in order to validate the
        input. Each validator will receive as an input the value returned from the previous
        validator. (As per the Pydantic help manual).
        """
        yield cls.validate

    @classmethod
    def validate(cls, value):
        """Custom validator to ensure that the value is a string object and that it matches
        the regex defined for users"""
        if not isinstance(value, str):
            raise TypeError('Unexpected type for URI: "%s"' % type(value))
        object_type = uri_regex.search(value.lower())
        if not object_type:
            raise ValueError(
                'Passed string value "%s" is not a well formatted '
                "MyTardis URI" % (value)
            )
        object_type = object_type.group(1)
        if not object_type.lower() in KNOWN_MYTARDIS_OBJECTS:
            raise ValueError(f'Unknown object type: "{object_type}"')
        return cls(f"{value}")

    def __repr__(self):
        """Indicate that it is a URI in the __repr__"""
        return f"URI({super().__repr__()})"


class ISODateTime(str):
    """Class type that handles datetime objects and returns a validated ISO date time
    formatted string. Validation is done against a regex taken from Stack Overflow.
    https://stackoverflow.com/questions/41129921/validate-an-iso-8601-datetime-string-in-python"""

    @classmethod
    def __get_validators__(cls):
        """One or more validators may be yieled which will be called in order to validate the
        input. Each validator will receive as an input the value returned from the previous
        validator. (As per the Pydantic help manual).
        """
        yield cls.validate

    @classmethod
    def validate(cls, value):
        """Custom validator to ensure that the value is a string object and that it matches
        the regex defined for an ISO 8601 formated datestime string"""
        if not isinstance(value, str):
            raise TypeError(
                'Unexpected type for ISO date/time stamp: "%s"' % type(value)
            )
        match = iso_time_regex.fullmatch(value)
        if not match:
            raise ValueError(
                'Passed string value "%s" is not an ISO 8601 formatted '
                "date/time string. Format should follow "
                "YYYY-MM-DDTHH:MM:SS.SSSSSS+HH:MM convention" % (value)
            )
        return cls(f"{value}")

    def __repr__(self):
        """Indicate that it is a formatted ISODateTime string via __repr__"""
        return f"ISODateTime({super().__repr__()})"


class BaseObjectType(str):
    """Class method that defines a validated string which includes the four object types
    found in MyTardis when projects are activated.This Type class is intended to be
    cassled by an Inner class defined in the Smelter/Crucible/Overseer and
    IngestionFactory classes to reduce the number of verificationss of project activation
    needed since the "project" key value will fail validation unless projects are
    active."""

    BASE_OBJECTS = [
        "experiment",
        "dataset",
        "datafile",
        "project",
    ]

    @classmethod
    def __get_validators__(cls):
        """One or more validators may be yieled which will be called in order to validate the
        input. Each validator will receive as an input the value returned from the previous
        validator. (As per the Pydantic help manual).
        """
        yield cls.validate

    @classmethod
    def validate(cls, value):
        """Custom validator to ensure that the string is one of the known objects in
        MyTardis."""
        if not isinstance(value, str):
            raise TypeError('Unexpected type for BaseObjectType "%s"' % type(value))
        if value not in cls.BASE_OBJECTS:
            raise ValueError(
                'Passed string value "%s" is not a recognised MyTardis '
                "object." % (value)
            )
        return cls(f"{value}")

    def __repr__(self):
        """Indicate that it is a formatted ISODateTime string via __repr__"""
        return f"BaseObjectType({super().__repr__()})"
