# pylint: disable=missing-function-docstring
"""Tests to validate the helper functions"""


import logging
import shutil
from pathlib import Path
from urllib.parse import urljoin

import mock
import pytest
from pytest import fixture
from requests import HTTPError, Request, RequestException, Response

from src.helpers import (
    MyTardisRESTFactory,
    SanityCheckError,
    read_json,
    sanity_check,
    write_json,
)
from src.helpers.config import MyTardisConnection
from src.helpers.mt_rest import MyTardisAuth

KB = 1024
MB = KB**2

logger = logging.getLogger(__name__)
logger.propagate = True

test_dict = {"key1": "data1", "key2": "data2", "key3": "data3"}
good_keys = ["key1", "key2"]
bad_keys = ["key1", "key2", "key4", "key5"]

json_dict = {"key1": "value1", "key2": "value2", "list1": ["a", "b", "c"], "int1": 1}


def test_sanity_checker_good():
    assert sanity_check("Good_Test", test_dict, good_keys)


def test_sanity_checker_bad():
    with pytest.raises(SanityCheckError):
        sanity_check("Bad_Test", test_dict, bad_keys)


def test_santity_checekr_str_not_list():
    assert sanity_check("Good Test", test_dict, "key1")


def test_sanity_checker_bad_str_not_list():
    with pytest.raises(SanityCheckError):
        sanity_check("Bad_Test", test_dict, "key4")


@pytest.mark.dependency()
def test_read_json(datadir):  # pylint: disable=redefined-outer-name
    test_file = Path(datadir / "known.json")
    assert read_json(test_file) == json_dict


@pytest.mark.dependency(depends=["test_read_json"])
def test_write_json(datadir):  # pylint: disable=redefined-outer-name
    output_file = Path(datadir / "test.json")
    write_json(json_dict, output_file)
    assert read_json(output_file) == json_dict


def test_mytardis_auth_header_injection(auth: MyTardisAuth):
    test_auth = MyTardisAuth(username=auth.username, api_key=auth.api_key)
    test_request = Request()
    test_auth(test_request)
    assert test_request.headers == {
        "Authorization": f"ApiKey {auth.username}:{auth.api_key}"
    }


def test_mytardis_rest_factory_setup(
    auth: MyTardisAuth, connection: MyTardisConnection
):
    test_factory = MyTardisRESTFactory(auth, connection)
    test_auth = MyTardisAuth(username=auth.username, api_key=auth.api_key)
    test_request = Request()
    assert test_factory.auth(test_request) == test_auth(test_request)
    assert test_factory.verify_certificate == connection.verify_certificate
    assert test_factory.api_template == connection.api_template
    assert test_factory.proxies == {
        "http": "http://myproxy.com",
        "https": "https://myproxy.com",
    }
    assert (
        test_factory.user_agent
        == "src.helpers.mt_rest/2.0 (https://github.com/UoA-eResearch/mytardis_ingestion.git)"
    )


@mock.patch("requests.request")
def test_backoff_on_mytardis_rest_factory_doesnt_trigger_on_httperror(
    mock_requests_request, auth: MyTardisAuth, connection: MyTardisConnection
):
    mock_response = Response()
    mock_response.status_code = 504
    mock_requests_request.return_value = mock_response
    test_factory = MyTardisRESTFactory(auth, connection)
    with pytest.raises(HTTPError):
        _ = test_factory.mytardis_api_request("GET", "http://example.com")
        assert mock_requests_request.call_count == 1


@mock.patch("requests.request")
def test_backoff_on_mytardis_rest_factory(
    mock_requests_request, auth: MyTardisAuth, connection: MyTardisConnection
):
    backoff_max_tries = 8
    # See backoff decorator on the mytardis_api_request function in MyTardisRESTFactory
    mock_response = Response()
    mock_response.status_code = 502
    mock_response.reason = "Test reason"
    mock_requests_request.return_value = mock_response
    test_factory = MyTardisRESTFactory(auth, connection)
    with pytest.raises(RequestException):
        _ = test_factory.mytardis_api_request("GET", "http://example.com")
    assert mock_requests_request.call_count == backoff_max_tries
