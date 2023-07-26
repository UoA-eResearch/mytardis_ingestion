# pylint: disable=missing-function-docstring,missing-module-docstring,missing-class-docstring
from datetime import datetime
from typing import List

import pytest
from dateutil import tz
from devtools import debug
from pydantic import BaseModel, ValidationError

from src.blueprints.custom_data_types import (
    KNOWN_MYTARDIS_OBJECTS,
    URI,
    ISODateTime,
    Username,
)

NZT = tz.gettz("Pacific/Auckland")


class DummyUsernames(BaseModel):
    user: Username


class DummyURI(BaseModel):
    uri: URI


class DummyISODateTime(BaseModel):
    iso_time: ISODateTime


def gen_uris() -> List[str]:
    return [f"/api/v1/{obj}/1/" for obj in KNOWN_MYTARDIS_OBJECTS]


@pytest.mark.parametrize("upis", ["test001", "ts001", "tst001"])
def test_UPI_is_good(upis: Username) -> None:  # pylint: disable=invalid-name
    test_class = DummyUsernames(user=upis)
    assert test_class.user == upis


def test_UPI_wrong_type() -> None:  # pylint: disable=invalid-name
    bad_upi = True
    with pytest.raises(ValidationError) as e_info:
        test_class = DummyUsernames(user=bad_upi)
        debug(test_class)
    debug(e_info.value)
    assert (
        "1 validation error for DummyUsernames\nuser\n  Input should be a valid string "
        "[type=string_type, input_value=True, input_type=bool]\n    "
    ) in str(e_info.value)


@pytest.mark.parametrize(
    "upis", ["totally_wrong", "t001", "tests001", "test01", "test0001"]
)
def test_malformed_UPI(upis: Username) -> None:  # pylint: disable=invalid-name
    with pytest.raises(ValidationError) as e_info:
        test_class = DummyUsernames(user=upis)
        debug(test_class)
    assert (
        (
            "1 validation error for DummyUsernames\nuser\n  "
            f'Value error, Passed string value "{upis}" '
            f"is not a well formatted Username [type=value_error, input_value='{upis}', "
            "input_type=str]\n    "
        )
    ) in str(e_info.value)


@pytest.mark.parametrize("uris", gen_uris())
def test_good_uris(uris: str) -> None:
    test_class = DummyURI(uri=uris)
    assert test_class.uri == uris


def test_URI_wrong_type() -> None:  # pylint: disable=invalid-name
    bad_uri = 1.23
    with pytest.raises(ValidationError) as e_info:
        _ = DummyURI(uri=bad_uri)
    assert (
        "1 validation error for DummyURI\nuri\n  Input should be a valid string "
        "[type=string_type, input_value=1.23, input_type=float]\n    "
    ) in str(e_info.value)


@pytest.mark.parametrize(
    "uris, expected_error",
    [
        (
            "totally wrong",
            (
                "1 validation error for DummyURI\nuri\n  Value error, "
                'Passed string value "totally wrong" '
                "is not a well formatted MyTardis URI "
                "[type=value_error, input_value='totally wrong', input_type=str]\n    "
                "For further information visit https://errors.pydantic.dev/2.0.3/v/value_error"
            ),
        ),
        (
            "/api/v1/banana/1/",
            (
                "1 validation error for DummyURI\nuri\n  Value error, Unknown object type: "
                "\"banana\" [type=value_error, input_value='/api/v1/banana/1/', "
                "input_type=str]\n    "
                "For further information visit https://errors.pydantic.dev/2.0.3/v/value_error"
            ),
        ),
        (
            "/api/v1/project",
            (
                "1 validation error for DummyURI\nuri\n  Value error, Passed string value "
                '"/api/v1/project" is not a well formatted MyTardis URI [type=value_error,'
                " input_value='/api/v1/project', input_type=str]\n    "
                "For further information visit https://errors.pydantic.dev/2.0.3/v/value_error"
            ),
        ),
        (
            "/api/v1/project/1/additional/",
            (
                "1 validation error for DummyURI\nuri\n  Value error, Passed string value "
                '"/api/v1/project/1/additional/" is not a well formatted MyTardis URI '
                "[type=value_error, input_value='/api/v1/project/1/additional/', "
                "input_type=str]\n    "
                "For further information visit https://errors.pydantic.dev/2.0.3/v/value_error"
            ),
        ),
    ],
)
def test_malformed_URIs(  # pylint: disable=invalid-name
    uris: str,
    expected_error: str,
) -> None:
    with pytest.raises(ValidationError) as e_info:
        _ = DummyURI(uri=uris)
    assert expected_error in str(e_info.value)


@pytest.mark.parametrize(
    "iso_strings, expected",
    (
        ("2022-01-01T12:00:00", "2022-01-01T12:00:00"),
        ("2022-01-01T12:00:00+12:00", "2022-01-01T12:00:00+12:00"),
        ("2022-01-01T12:00:00.0+12:00", "2022-01-01T12:00:00.0+12:00"),
        ("2022-01-01T12:00:00.00+12:00", "2022-01-01T12:00:00.00+12:00"),
        ("2022-01-01T12:00:00.000+12:00", "2022-01-01T12:00:00.000+12:00"),
        ("2022-01-01T12:00:00.0000+12:00", "2022-01-01T12:00:00.0000+12:00"),
        ("2022-01-01T12:00:00.00000+12:00", "2022-01-01T12:00:00.00000+12:00"),
        (
            "2022-01-01T12:00:00.000000+12:00",
            "2022-01-01T12:00:00.000000+12:00",
        ),
        (
            datetime(2022, 1, 1, 12, 00, 00, 000000).isoformat(),
            "2022-01-01T12:00:00",
        ),
        (datetime(2022, 1, 1, tzinfo=NZT).isoformat(), "2022-01-01T00:00:00+13:00"),
        (
            datetime(2022, 1, 1, 12, 00, 00, tzinfo=NZT).isoformat(),
            "2022-01-01T12:00:00+13:00",
        ),
    ),
)
def test_good_ISO_DateTime_string(  # pylint: disable=invalid-name
    iso_strings: str,
    expected: str,
) -> None:
    test_class = DummyISODateTime(iso_time=iso_strings)
    assert test_class.iso_time == expected
