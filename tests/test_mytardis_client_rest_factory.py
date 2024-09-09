# pylint: disable=missing-function-docstring
# nosec assert_used
# flake8: noqa S101
"""Tests to validate the helper functions"""


import logging
from typing import Any

import mock
import pytest
import responses
from mock import MagicMock
from requests import HTTPError, Request, RequestException, Response
from responses import matchers
from tenacity import wait_fixed

from src.blueprints.datafile import Datafile
from src.config.config import AuthConfig, ConnectionConfig
from src.mytardis_client.endpoints import URI, MyTardisEndpoint
from src.mytardis_client.mt_rest import (
    GetRequestMetaParams,
    GetResponseMeta,
    MyTardisRESTFactory,
    endpoint_is_not,
    sanitize_params,
)
from src.mytardis_client.response_data import IngestedDatafile
from src.utils.types.type_helpers import is_list_of

logger = logging.getLogger(__name__)
logger.propagate = True


def test_mytardis_auth_header_injection(auth: AuthConfig) -> None:
    test_auth = AuthConfig(username=auth.username, api_key=auth.api_key)
    test_request = Request()
    test_auth(test_request)  # type: ignore
    assert test_request.headers == {
        "Authorization": f"ApiKey {auth.username}:{auth.api_key}"
    }


def test_mytardis_rest_factory_setup(
    auth: AuthConfig, connection: ConnectionConfig
) -> None:
    test_factory = MyTardisRESTFactory(auth, connection)
    test_auth = AuthConfig(username=auth.username, api_key=auth.api_key)
    test_request = Request()
    assert test_factory.auth(test_request) == test_auth(test_request)  # type: ignore
    assert test_factory.verify_certificate == connection.verify_certificate
    assert test_factory.proxies["http"] == "http://myproxy.com"  # type: ignore
    assert test_factory.proxies["https"] == "http://myproxy.com"  # type: ignore
    assert (
        test_factory.user_agent
        == "src.mytardis_client.mt_rest/2.0 (https://github.com/UoA-eResearch/mytardis_ingestion.git)"
    )


@mock.patch("requests.Session.request")
def test_retry_on_mytardis_rest_factory_doesnt_trigger_on_httperror(
    mock_requests_request: MagicMock, auth: AuthConfig, connection: ConnectionConfig
) -> None:
    mock_response = Response()
    mock_response.status_code = 504
    mock_requests_request.return_value = mock_response
    test_factory = MyTardisRESTFactory(auth, connection)
    with pytest.raises(HTTPError):
        _ = test_factory.request("GET", "/project")
        assert mock_requests_request.call_count == 1


@mock.patch("requests.Session.request")
@pytest.mark.long
def test_retry_on_mytardis_rest_factory(
    mock_requests_request: MagicMock, auth: AuthConfig, connection: ConnectionConfig
) -> None:
    backoff_max_tries = 8
    # See backoff decorator on the mytardis_api_request function in MyTardisRESTFactory
    mock_response = Response()
    mock_response.status_code = 502
    mock_response.reason = "Test reason"
    mock_requests_request.return_value = mock_response
    test_factory = MyTardisRESTFactory(auth, connection)

    # Disable the exponential backoff wait time to keep the test duration short.
    # This uses a facility from tenacity (retry field added by the decorator).
    test_factory.request.retry.wait = wait_fixed(0.1)  # type: ignore[attr-defined]

    with pytest.raises(RequestException):
        _ = test_factory.request("GET", "/project")
    assert mock_requests_request.call_count == backoff_max_tries


@responses.activate
def test_mytardis_client_rest_get_single(
    datafile_get_response_single: dict[str, Any],
    auth: AuthConfig,
    connection: ConnectionConfig,
) -> None:
    mt_client = MyTardisRESTFactory(auth, connection)

    responses.add(
        responses.GET,
        mt_client.compose_url("/dataset_file"),
        status=200,
        json=datafile_get_response_single,
    )

    datafiles, meta = mt_client.get("/dataset_file")

    assert isinstance(datafiles, list)
    assert is_list_of(datafiles, IngestedDatafile)
    assert len(datafiles) == 1
    assert datafiles[0].resource_uri == URI("/api/v1/dataset_file/0/")
    assert datafiles[0].filename == "test_filename.txt"

    assert isinstance(meta, GetResponseMeta)
    assert meta.total_count == 1


@responses.activate
def test_mytardis_client_rest_get_multi(
    datafile_get_response_multi: dict[str, Any],
    auth: AuthConfig,
    connection: ConnectionConfig,
) -> None:
    mt_client = MyTardisRESTFactory(auth, connection)

    responses.add(
        responses.GET,
        mt_client.compose_url("/dataset_file"),
        status=200,
        json=datafile_get_response_multi,
    )

    datafiles, meta = mt_client.get(
        "/dataset_file",
        meta_params=GetRequestMetaParams(offset=2, limit=3),
    )

    assert is_list_of(datafiles, IngestedDatafile)
    assert len(datafiles) == 30
    assert (isinstance(df, Datafile) for df in datafiles)

    assert datafiles[0].resource_uri == URI("/api/v1/dataset_file/0/")
    assert datafiles[1].resource_uri == URI("/api/v1/dataset_file/1/")
    assert datafiles[2].resource_uri == URI("/api/v1/dataset_file/2/")

    assert datafiles[0].filename == "test_filename_0.txt"
    assert datafiles[1].filename == "test_filename_1.txt"
    assert datafiles[2].filename == "test_filename_2.txt"

    assert isinstance(meta, GetResponseMeta)
    assert meta.total_count == 30
    assert meta.limit == 30
    assert meta.offset == 0


@responses.activate
def test_mytardis_client_rest_get_all(
    datafile_get_response_paginated_first: dict[str, Any],
    datafile_get_response_paginated_second: dict[str, Any],
    auth: AuthConfig,
    connection: ConnectionConfig,
) -> None:
    mt_client = MyTardisRESTFactory(auth, connection)

    responses.add(
        responses.GET,
        mt_client.compose_url("/dataset_file"),
        status=200,
        json=datafile_get_response_paginated_first,
    )
    responses.add(
        responses.GET,
        mt_client.compose_url("/dataset_file"),
        status=200,
        json=datafile_get_response_paginated_second,
    )

    datafiles, total_count = mt_client.get_all(
        "/dataset_file",
        batch_size=20,
    )

    assert is_list_of(datafiles, IngestedDatafile)
    assert len(datafiles) == 30
    assert (isinstance(df, Datafile) for df in datafiles)

    for i in range(30):
        assert datafiles[i].resource_uri == URI(f"/api/v1/dataset_file/{i}/")
        assert datafiles[i].filename == f"test_filename_{i}.txt"

    assert total_count == 30


@pytest.mark.parametrize(
    "input_params,expected_sanitized",
    [
        pytest.param(
            {"string": "Hello", "int": 12, "dataset": URI("/api/v1/dataset/34/")},
            {"string": "Hello", "int": 12, "dataset": 34},
            id="no-nesting",
        ),
        pytest.param(
            {
                "string": "Hello",
                "nested_dict": {"string": "Foo", "dataset": URI("/api/v1/dataset/34/")},
            },
            {
                "string": "Hello",
                "nested_dict": {"string": "Foo", "dataset": 34},
            },
            id="nested-dict",
        ),
        pytest.param(
            {
                "string": "Foo",
                "nested_list": [URI("/api/v1/dataset/10/"), URI("/api/v1/dataset/11/")],
            },
            {
                "string": "Foo",
                "nested_list": [10, 11],
            },
            id="nested-list",
        ),
        pytest.param(
            {
                "string": "Foo",
                "nested_list": [URI("/api/v1/dataset/10/"), URI("/api/v1/dataset/11/")],
                "nested_dict": {"string": "Foo", "dataset": URI("/api/v1/dataset/34/")},
            },
            {
                "string": "Foo",
                "nested_list": [10, 11],
                "nested_dict": {"string": "Foo", "dataset": 34},
            },
            id="nested-list-and-dict",
        ),
        pytest.param(
            {
                "string": "Foo",
                "nested_list_of_dicts": [
                    {"string": "Foo", "dataset": URI("/api/v1/dataset/34/")},
                    {"string": "Bar", "dataset": URI("/api/v1/dataset/35/")},
                ],
                "nested_dict_with_list": {
                    "string": "Foo",
                    "datasets": [
                        URI("/api/v1/dataset/34/"),
                        URI("/api/v1/dataset/35/"),
                    ],
                },
            },
            {
                "string": "Foo",
                "nested_list_of_dicts": [
                    {"string": "Foo", "dataset": 34},
                    {"string": "Bar", "dataset": 35},
                ],
                "nested_dict_with_list": {
                    "string": "Foo",
                    "datasets": [34, 35],
                },
            },
            id="doubly-nested",
        ),
        pytest.param(
            {
                "uri_string": "/api/v1/dataset/34/",
                "uri_strings": ["/api/v1/dataset/34/", "/api/v1/dataset/35/"],
            },
            {
                "uri_string": 34,
                "uri_strings": [34, 35],
            },
            id="uris-stored-as-strings",
        ),
    ],
)
def test_mytardis_client_sanitize_params(
    input_params: dict[str, Any], expected_sanitized: dict[str, Any]
) -> None:

    assert sanitize_params(input_params) == expected_sanitized


@responses.activate
def test_mytardis_client_get_params_are_sanitized(
    datafile_get_response_single: dict[str, Any],
    auth: AuthConfig,
    connection: ConnectionConfig,
) -> None:

    mt_client = MyTardisRESTFactory(auth, connection)

    responses.get(
        mt_client.compose_url("/dataset_file"),
        status=200,
        json=datafile_get_response_single,
        match=[matchers.query_param_matcher({"limit": 1, "offset": 0, "dataset": 0})],
    )

    _ = mt_client.get(
        "/dataset_file",
        query_params={"dataset": URI("/api/v1/dataset/0/")},
        meta_params=GetRequestMetaParams(limit=1, offset=0),
    )


@pytest.mark.parametrize(
    "endpoints,url,expected_output",
    [
        pytest.param(("/dataset_file",), "https://example.com/dataset_file", False),
        pytest.param(("/dataset",), "https://example.com/dataset_file/", True),
        pytest.param(("/dataset",), "https://example.com/dataset/", False),
        pytest.param(("/dataset",), "https://example.com/dataset", False),
        pytest.param(
            (
                "/dataset",
                "/dataset_file",
            ),
            "https://example.com/dataset",
            False,
        ),
    ],
)
def test_make_endpoint_filter(
    endpoints: tuple[MyTardisEndpoint], url: str, expected_output: bool
) -> None:

    response = MagicMock()
    response.url = url

    assert endpoint_is_not(endpoints)(response) == expected_output
