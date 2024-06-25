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

from src.blueprints.datafile import Datafile
from src.config.config import AuthConfig, ConnectionConfig
from src.mytardis_client.data_types import URI
from src.mytardis_client.mt_rest import (
    GetRequestMetaParams,
    GetResponseMeta,
    MyTardisRESTFactory,
    sanitize_params,
)

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
def test_backoff_on_mytardis_rest_factory_doesnt_trigger_on_httperror(
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
def test_backoff_on_mytardis_rest_factory(
    mock_requests_request: MagicMock, auth: AuthConfig, connection: ConnectionConfig
) -> None:
    backoff_max_tries = 8
    # See backoff decorator on the mytardis_api_request function in MyTardisRESTFactory
    mock_response = Response()
    mock_response.status_code = 502
    mock_response.reason = "Test reason"
    mock_requests_request.return_value = mock_response
    test_factory = MyTardisRESTFactory(auth, connection)
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

    datafiles, meta = mt_client.get("/dataset_file", object_type=Datafile)

    assert isinstance(datafiles, list)
    assert len(datafiles) == 1
    assert datafiles[0].resource_uri == URI("/api/v1/dataset_file/0/")
    assert datafiles[0].obj.filename == "test_filename.txt"

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
        object_type=Datafile,
        meta_params=GetRequestMetaParams(offset=2, limit=3),
    )

    assert isinstance(datafiles, list)
    assert len(datafiles) == 30
    assert (isinstance(df, Datafile) for df in datafiles)

    assert datafiles[0].resource_uri == URI("/api/v1/dataset_file/0/")
    assert datafiles[1].resource_uri == URI("/api/v1/dataset_file/1/")
    assert datafiles[2].resource_uri == URI("/api/v1/dataset_file/2/")

    assert datafiles[0].obj.filename == "test_filename_0.txt"
    assert datafiles[1].obj.filename == "test_filename_1.txt"
    assert datafiles[2].obj.filename == "test_filename_2.txt"

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
        object_type=Datafile,
        batch_size=20,
    )

    assert isinstance(datafiles, list)
    assert len(datafiles) == 30
    assert (isinstance(df, Datafile) for df in datafiles)

    for i in range(30):
        assert datafiles[i].resource_uri == URI(f"/api/v1/dataset_file/{i}/")
        assert datafiles[i].obj.filename == f"test_filename_{i}.txt"

    assert total_count == 30


def test_mytardis_client_sanitize_params() -> None:

    sanitized = sanitize_params(
        {
            "string": "Hello",
            "int": 12,
            "uri": URI("/api/v1/dataset/34/"),
        }
    )
    assert sanitized == {
        "string": "Hello",
        "int": 12,
        "uri": 34,
    }


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
        object_type=Datafile,
        query_params={"dataset": URI("/api/v1/dataset/0/")},
        meta_params=GetRequestMetaParams(limit=1, offset=0),
    )
