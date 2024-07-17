"""Common types used in the MyTardis client library."""

import re
from typing import Annotated, Any

from pydantic import AfterValidator, PlainSerializer, WithJsonSchema

iso_time_regex = re.compile(
    r"^(-?(?:[1-9][0-9]*)?[0-9]{4})-(1[0-2]|0[1-9])-(3[01]|0[1-9]|[12][0-9])T(2[0-3]|[01][0-9]):([0-5][0-9]):([0-5][0-9])(\.[0-9]+)?(Z|[+-](?:2[0-3]|[01][0-9]):[0-5][0-9])?$"  # pylint: disable=line-too-long
)
iso_date_regex = re.compile(
    r"^(-?(?:[1-9][0-9]*)?[0-9]{4})-(1[0-2]|0[1-9])-(3[01]|0[1-9]|[12][0-9])"
)


def validate_isodatetime(value: Any) -> str:
    """Custom validator to ensure that the value is a string object and that it matches
    the regex defined for an ISO 8601 formatted datetime string"""
    if not isinstance(value, str):
        raise TypeError(f'Unexpected type for ISO date/time stamp: "{type(value)}"')
    if match := iso_time_regex.fullmatch(value):
        return f"{match.group(0)}"
    raise ValueError(
        'Passed string value "%s" is not an ISO 8601 formatted '
        "date/time string. Format should follow "
        "YYYY-MM-DDTHH:MM:SS.SSSSSS+HH:MM convention" % (value)
    )


ISODateTime = Annotated[
    str,
    AfterValidator(validate_isodatetime),
    PlainSerializer(lambda x: str(x), return_type=str),
    WithJsonSchema({"type": "string"}, mode="serialization"),
]
