# pylint: disable=missing-function-docstring
"""Tests to validate the helper functions"""


import logging

import mock
import pytest
from requests import HTTPError, Request, RequestException, Response

from src.helpers.config import ConnectionConfig
from src.helpers.mt_rest import AuthConfig, MyTardisRESTFactory

logger = logging.getLogger(__name__)
logger.propagate = True


def test_mytardis_auth_header_injection(auth: AuthConfig):
    test_auth = AuthConfig(username=auth.username, api_key=auth.api_key)
    test_request = Request()
    test_auth(test_request)
    assert test_request.headers == {
        "Authorization": f"ApiKey {auth.username}:{auth.api_key}"
    }


def test_mytardis_rest_factory_setup(
    auth: AuthConfig, connection: ConnectionConfig
):
    test_factory = MyTardisRESTFactory(auth, connection)
    test_auth = AuthConfig(username=auth.username, api_key=auth.api_key)
    test_request = Request()
    assert test_factory.auth(test_request) == test_auth(test_request)
    assert test_factory.verify_certificate == connection.verify_certificate
    assert test_factory.api_template == connection.api_template
    assert test_factory.proxies["http"] == "http://myproxy.com"
    assert test_factory.proxies["https"] == "http://myproxy.com"
    assert (
        test_factory.user_agent
        == "src.helpers.mt_rest/2.0 (https://github.com/UoA-eResearch/mytardis_ingestion.git)"
    )


@mock.patch("requests.request")
def test_backoff_on_mytardis_rest_factory_doesnt_trigger_on_httperror(
    mock_requests_request, auth: AuthConfig, connection: ConnectionConfig
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
    mock_requests_request, auth: AuthConfig, connection: ConnectionConfig
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
