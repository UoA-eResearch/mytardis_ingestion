from datetime import datetime

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


def gen_uris():
    uris = []
    for obj in KNOWN_MYTARDIS_OBJECTS:
        uris.append(f"/api/v1/{obj}/1/")
    return uris


@pytest.mark.parametrize("upis", ["test001", "ts001", "tst001"])
def test_UPI_is_good(upis):
    test_class = DummyUsernames(user=upis)
    assert test_class.user == upis


def test_UPI_wrong_type():
    bad_upi = True
    with pytest.raises(ValidationError) as e_info:
        test_class = DummyUsernames(user=bad_upi)
        debug(test_class)
    debug(e_info.value)
    assert (
        str(e_info.value)
        == "1 validation error for DummyUsernames\nuser\n  Unexpected type for Username: \"<class 'bool'>\" (type=type_error)"
    )


@pytest.mark.parametrize(
    "upis", ["totally_wrong", "t001", "tests001", "test01", "test0001"]
)
def test_malformed_UPI(upis):
    with pytest.raises(ValidationError) as e_info:
        test_class = DummyUsernames(user=upis)
        debug(test_class)
    assert str(e_info.value) == (
        f'1 validation error for DummyUsernames\nuser\n  Passed string value "{upis}" is not a well formatted Username (type=value_error)'
    )


@pytest.mark.parametrize("uris", gen_uris())
def test_good_uris(uris):
    test_class = DummyURI(uri=uris)
    assert test_class.uri == uris


def test_URI_wrong_type():
    bad_uri = 1.23
    with pytest.raises(ValidationError) as e_info:
        test_class = DummyURI(uri=bad_uri)
    assert (
        str(e_info.value)
        == "1 validation error for DummyURI\nuri\n  Unexpected type for URI: \"<class 'float'>\" (type=type_error)"
    )


@pytest.mark.parametrize(
    "uris, expected_error",
    [
        (
            "totally wrong",
            '1 validation error for DummyURI\nuri\n  Passed string value "totally wrong" is not a well formatted MyTardis URI (type=value_error)',
        ),
        (
            "/api/v1/banana/1/",
            '1 validation error for DummyURI\nuri\n  Unknown object type: "banana" (type=value_error)',
        ),
        (
            "/api/v1/project",
            '1 validation error for DummyURI\nuri\n  Passed string value "/api/v1/project" is not a well formatted MyTardis URI (type=value_error)',
        ),
        (
            "/api/v1/project/1/additional/",
            '1 validation error for DummyURI\nuri\n  Passed string value "/api/v1/project/1/additional/" is not a well formatted MyTardis URI (type=value_error)',
        ),
    ],
)
def test_malformed_URIs(uris, expected_error):
    with pytest.raises(ValidationError) as e_info:
        test_class = DummyURI(uri=uris)
    assert str(e_info.value) == expected_error


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
def test_good_ISO_DateTime_string(iso_strings, expected):
    test_class = DummyISODateTime(iso_time=iso_strings)
    assert test_class.iso_time == expected
