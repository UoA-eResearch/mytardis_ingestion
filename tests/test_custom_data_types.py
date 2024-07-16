# pylint: disable=missing-function-docstring,missing-module-docstring
# pylint: disable=missing-class-docstring
# nosec assert_used
# flake8: noqa S101

from datetime import datetime

import pytest
from dateutil import tz
from pydantic import BaseModel

from src.blueprints.custom_data_types import ISODateTime, Username

NZT = tz.gettz("Pacific/Auckland")


class DummyUsernames(BaseModel):
    user: Username


class DummyISODateTime(BaseModel):
    iso_time: ISODateTime


@pytest.mark.parametrize("upis", ["test001", "ts001", "tst001"])
def test_UPI_is_good(upis: Username) -> None:  # pylint: disable=invalid-name
    test_class = DummyUsernames(user=upis)
    assert test_class.user == upis


def test_UPI_wrong_type() -> None:  # pylint: disable=invalid-name
    bad_upi = True
    with pytest.raises(ValueError) as e_info:
        _ = DummyUsernames(user=bad_upi)
    assert (
        "1 validation error for DummyUsernames\nuser\n  Input should be a valid string "
        "[type=string_type, input_value=True, input_type=bool]\n    "
    ) in str(e_info.value)


@pytest.mark.parametrize(
    "upis", ["totally_wrong", "t001", "tests001", "test01", "test0001"]
)
def test_malformed_UPI(upis: Username) -> None:  # pylint: disable=invalid-name
    with pytest.raises(ValueError) as e_info:
        _ = DummyUsernames(user=upis)
    assert (
        (
            "1 validation error for DummyUsernames\nuser\n  "
            f'Value error, Passed string value "{upis}" '
            f"is not a well formatted Username [type=value_error, input_value='{upis}', "
            "input_type=str]\n    "
        )
    ) in str(e_info.value)


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
