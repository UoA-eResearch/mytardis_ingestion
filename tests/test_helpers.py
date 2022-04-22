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
from src.helpers.mt_rest import MyTardisAuth

KB = 1024
MB = KB**2

logger = logging.getLogger(__name__)
logger.propagate = True

config_dict = {
    "username": "Test_User",
    "api_key": "Test_API_Key",
    "hostname": "https://test.mytardis.nectar.auckland.ac.nz",
    "verify_certificate": True,
    "proxy_http": "http://myproxy.com",
    "proxy_https": "http://myproxy.com",
}

test_dict = {"key1": "data1", "key2": "data2", "key3": "data3"}
good_keys = ["key1", "key2"]
bad_keys = ["key1", "key2", "key4", "key5"]

json_dict = {"key1": "value1", "key2": "value2", "list1": ["a", "b", "c"], "int1": 1}


@fixture
def datadir(tmpdir, request):
    """
    Fixture responsible for searching a folder with the same name of test
    module and, if available, moving all contents to a temporary directory so
    tests can use them freely.
    """
    filename = request.module.__file__
    test_dir = Path(filename).with_suffix("")

    if test_dir.is_dir():
        for source in test_dir.glob("*"):
            shutil.copy(source, tmpdir)

    return tmpdir


def test_sanity_checker_good():
    assert sanity_check("Good_Test", test_dict, good_keys)


def test_sanity_checker_bad():
    with pytest.raises(SanityCheckError):
        sanity_check("Bad_Test", test_dict, bad_keys)


@pytest.mark.dependency()
def test_read_json(datadir):  # pylint: disable=redefined-outer-name
    test_file = Path(datadir / "known.json")
    assert read_json(test_file) == json_dict


@pytest.mark.dependency(depends=["test_read_json"])
def test_write_json(datadir):  # pylint: disable=redefined-outer-name
    output_file = Path(datadir / "test.json")
    write_json(json_dict, output_file)
    assert read_json(output_file) == json_dict


def test_mytardis_auth_header_injection():
    test_auth = MyTardisAuth(
        username=config_dict["username"], api_key=config_dict["api_key"]
    )
    test_request = Request()
    test_auth(test_request)
    assert test_request.headers == {
        "Authorization": f"ApiKey {config_dict['username']}:{config_dict['api_key']}"
    }


def test_mytardis_rest_factory_setup():
    test_factory = MyTardisRESTFactory(config_dict)
    test_auth = MyTardisAuth(
        username=config_dict["username"], api_key=config_dict["api_key"]
    )
    test_request = Request()
    assert test_factory.auth(test_request) == test_auth(test_request)
    assert test_factory.verify_certificate == config_dict["verify_certificate"]
    assert test_factory.api_template == urljoin(config_dict["hostname"], "/api/v1/")
    assert test_factory.proxies == {
        "http": "http://myproxy.com",
        "https": "http://myproxy.com",
    }
    assert (
        test_factory.user_agent
        == "src.helpers.mt_rest/2.0 (https://github.com/UoA-eResearch/mytardis_ingestion.git)"
    )


@mock.patch("requests.request")
def test_backoff_on_mytardis_rest_factory_doesnt_trigger_on_httperror(
    mock_requests_request,
):
    mock_response = Response()
    mock_response.status_code = 504
    mock_requests_request.return_value = mock_response
    test_factory = MyTardisRESTFactory(config_dict)
    with pytest.raises(HTTPError):
        _ = test_factory.mytardis_api_request("GET", "http://example.com")
        assert mock_requests_request.call_count == 1


@mock.patch("requests.request")
def test_backoff_on_mytardis_rest_factory(mock_requests_request):
    backoff_max_tries = 8
    # See backoff decorator on the mytardis_api_request function in MyTardisRESTFactory
    mock_response = Response()
    mock_response.status_code = 502
    mock_response.reason = "Test reason"
    mock_requests_request.return_value = mock_response
    test_factory = MyTardisRESTFactory(config_dict)
    with pytest.raises(RequestException):
        _ = test_factory.mytardis_api_request("GET", "http://example.com")
    assert mock_requests_request.call_count == backoff_max_tries
