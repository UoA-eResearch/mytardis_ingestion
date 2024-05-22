# pylint: disable=missing-function-docstring
# nosec assert_used
# flake8: noqa S101

"""Tests of the Overseer class and its functions"""
import logging
from typing import Any
from urllib.parse import urljoin

import mock
import pytest
import responses
from pytest import LogCaptureFixture
from requests import HTTPError
from responses import matchers

from src.blueprints.custom_data_types import URI
from src.config.config import ConnectionConfig, IntrospectionConfig
from src.mytardis_client.endpoints import get_endpoint
from src.mytardis_client.helpers import resource_uri_to_id
from src.mytardis_client.mt_rest import MyTardisRESTFactory
from src.mytardis_client.objects import MyTardisObject
from src.overseers.overseer import Overseer

logger = logging.getLogger(__name__)
logger.propagate = True


@pytest.fixture(name="overseer_plain")
def fixture_overseer_plain(
    rest_factory: MyTardisRESTFactory,
) -> Any:
    return Overseer(rest_factory)


def test_staticmethod_resource_uri_to_id() -> None:
    test_uri = URI("/v1/user/10")
    assert resource_uri_to_id(test_uri) == 10

    test_uri = URI("/v1/user/10/")
    assert resource_uri_to_id(test_uri) == 10


@responses.activate
def test_get_matches_from_mytardis(
    overseer: Overseer,
    connection: ConnectionConfig,
    project_response_dict: dict[str, Any],
) -> None:
    object_type = MyTardisObject.PROJECT
    project_name = project_response_dict["objects"][0]["name"]
    project_identifiers = project_response_dict["objects"][0]["identifiers"]

    endpoint = get_endpoint(object_type)
    responses.add(
        responses.GET,
        urljoin(connection.api_template, endpoint.url_suffix),
        json=(project_response_dict),
        match=[
            matchers.query_param_matcher(
                {"name": project_name},
            ),
        ],
        status=200,
    )
    responses.add(
        responses.GET,
        urljoin(connection.api_template, endpoint.url_suffix),
        json=(project_response_dict),
        match=[
            matchers.query_param_matcher(
                {"identifier": project_identifiers[0]},
            ),
        ],
        status=200,
    )

    # pylint: disable=protected-access
    assert (
        overseer._get_matches_from_mytardis(
            object_type,
            {"name": project_name},
        )
        == project_response_dict["objects"]
    )

    # pylint: disable=protected-access
    assert (
        overseer._get_matches_from_mytardis(
            object_type,
            {"identifier": project_identifiers[0]},
        )
        == project_response_dict["objects"]
    )

    Overseer.clear()


@responses.activate
def test_get_objects_http_error(
    caplog: LogCaptureFixture,
    connection: ConnectionConfig,
    overseer: Overseer,
) -> None:
    object_type = MyTardisObject.PROJECT
    search_string = "Project_1"
    url_suffix = get_endpoint(object_type).url_suffix
    responses.add(
        responses.GET,
        urljoin(connection.api_template, url_suffix),
        match=[
            matchers.query_param_matcher(
                {"name": search_string},
            ),
        ],
        status=504,
    )
    responses.add(
        responses.GET,
        urljoin(connection.api_template, url_suffix),
        match=[
            matchers.query_param_matcher(
                {"identifier": search_string},
            ),
        ],
        status=504,
    )
    caplog.set_level(logging.WARNING)

    with pytest.raises(HTTPError):
        overseer.get_matching_objects(object_type, {"name": search_string})

    assert "Failed HTTP request" in caplog.text
    assert "Overseer" in caplog.text
    assert f"{object_type}" in caplog.text

    Overseer.clear()


@mock.patch("src.mytardis_client.mt_rest.MyTardisRESTFactory.mytardis_api_request")
def test_get_objects_general_error(
    mock_mytardis_api_request: Any,
    caplog: LogCaptureFixture,
    overseer: Overseer,
) -> None:
    mock_mytardis_api_request.side_effect = IOError()
    object_type = MyTardisObject.PROJECT
    search_string = "Project_1"
    error_str = (
        "Non-HTTP exception in Overseer.get_objects call\n"
        f"object_type = {object_type}\n"
        "query_params"
    )
    with pytest.raises(IOError):
        _ = overseer.get_matching_objects(object_type, {"name": search_string})
        assert error_str in caplog.text

    Overseer.clear()


@responses.activate
def test_get_objects_no_objects(
    overseer: Overseer,
    connection: ConnectionConfig,
    response_dict_not_found: dict[str, Any],
) -> None:
    object_type = MyTardisObject.PROJECT
    endpoint = get_endpoint(object_type)
    search_string = "Project_1"
    responses.add(
        responses.GET,
        urljoin(connection.api_template, endpoint.url_suffix),
        json=(response_dict_not_found),
        match=[
            matchers.query_param_matcher(
                {"name": search_string},
            ),
        ],
        status=200,
    )
    responses.add(
        responses.GET,
        urljoin(connection.api_template, endpoint.url_suffix),
        json=(response_dict_not_found),
        match=[
            matchers.query_param_matcher(
                {"identifier": search_string},
            ),
        ],
        status=200,
    )

    assert overseer.get_matching_objects(object_type, {"name": search_string}) == []

    Overseer.clear()


@responses.activate
def test_get_uris(
    connection: ConnectionConfig,
    overseer: Overseer,
    project_response_dict: dict[str, Any],
) -> None:
    object_type = MyTardisObject.PROJECT
    endpoint = get_endpoint(object_type)
    search_string = "Project_1"

    responses.add(
        responses.GET,
        urljoin(connection.api_template, endpoint.url_suffix),
        json=(project_response_dict),
        match=[
            matchers.query_param_matcher(
                {"identifier": search_string},
            ),
        ],
        status=200,
    )

    assert overseer.get_uris_by_identifier(
        MyTardisObject.PROJECT,
        search_string,
    ) == [URI("/api/v1/project/1/")]

    Overseer.clear()


@responses.activate
def test_get_uris_no_objects(
    connection: ConnectionConfig,
    overseer: Overseer,
    response_dict_not_found: dict[str, Any],
) -> None:
    object_type = MyTardisObject.PROJECT
    endpoint = get_endpoint(object_type)
    search_string = "Project_1"
    responses.add(
        responses.GET,
        urljoin(connection.api_template, endpoint.url_suffix),
        json=(response_dict_not_found),
        match=[
            matchers.query_param_matcher(
                {"name": search_string},
            ),
        ],
        status=200,
    )
    responses.add(
        responses.GET,
        urljoin(connection.api_template, endpoint.url_suffix),
        json=(response_dict_not_found),
        match=[
            matchers.query_param_matcher(
                {"identifier": search_string},
            ),
        ],
        status=200,
    )
    assert (
        overseer.get_uris_by_identifier(
            MyTardisObject.PROJECT,
            search_string,
        )
        == []
    )
    Overseer.clear()


@responses.activate
def test_get_uris_malformed_return_dict(
    caplog: LogCaptureFixture,
    connection: ConnectionConfig,
    overseer: Overseer,
    project_response_dict: dict[str, Any],
) -> None:
    caplog.set_level(logging.ERROR)
    test_dict = project_response_dict
    test_dict["objects"][0].pop("resource_uri")
    object_type = MyTardisObject.PROJECT
    endpoint = get_endpoint(object_type)
    search_string = "Project_1"
    responses.add(
        responses.GET,
        urljoin(connection.api_template, endpoint.url_suffix),
        json=(test_dict),
        match=[
            matchers.query_param_matcher(
                {"name": search_string},
            ),
        ],
        status=200,
    )
    responses.add(
        responses.GET,
        urljoin(connection.api_template, endpoint.url_suffix),
        json=(test_dict),
        match=[
            matchers.query_param_matcher(
                {"identifier": search_string},
            ),
        ],
        status=200,
    )
    error_str = (
        "Malformed return from MyTardis. No resource_uri found for "
        f"{object_type} searching with {search_string}"
    )
    with pytest.raises(KeyError):
        _ = overseer.get_uris_by_identifier(
            MyTardisObject.PROJECT,
            search_string,
        )
        assert error_str in caplog.text
    Overseer.clear()


@responses.activate
def test_get_uris_ensure_http_errors_caught_by_get_objects(
    caplog: LogCaptureFixture,
    connection: ConnectionConfig,
    overseer: Overseer,
) -> None:
    caplog.set_level(logging.WARNING)
    object_type = MyTardisObject.PROJECT
    search_string = "Project_1"
    endpoint = get_endpoint(object_type)
    responses.add(
        responses.GET,
        urljoin(connection.api_template, endpoint.url_suffix),
        match=[
            matchers.query_param_matcher(
                {"name": search_string},
            ),
        ],
        status=504,
    )
    responses.add(
        responses.GET,
        urljoin(connection.api_template, endpoint.url_suffix),
        match=[
            matchers.query_param_matcher(
                {"identifier": search_string},
            ),
        ],
        status=504,
    )

    with pytest.raises(HTTPError):
        overseer.get_uris_by_identifier(
            MyTardisObject.PROJECT,
            search_string,
        )

    assert "Failed HTTP request from Overseer" in caplog.text
    assert f"{object_type}" in caplog.text

    Overseer.clear()


@mock.patch("src.mytardis_client.mt_rest.MyTardisRESTFactory.mytardis_api_request")
def test_get_uris_general_error(
    mock_mytardis_api_request: Any,
    overseer: Overseer,
) -> None:
    mock_mytardis_api_request.side_effect = IOError()
    object_type = MyTardisObject.PROJECT
    search_string = "Project_1"
    with pytest.raises(IOError):
        _ = overseer.get_uris_by_identifier(
            object_type,
            search_string,
        )
    Overseer.clear()


@responses.activate
def test_get_mytardis_setup(
    overseer_plain: Overseer,
    connection: ConnectionConfig,
    introspection_response_dict: dict[str, Any],
    mytardis_setup: IntrospectionConfig,
) -> None:
    overseer_plain._mytardis_setup = None  # pylint: disable=protected-access
    assert overseer_plain._mytardis_setup is None  # pylint: disable=protected-access

    responses.add(
        responses.GET,
        urljoin(
            connection.api_template,
            "introspection",
        ),
        json=(introspection_response_dict),
        status=200,
    )

    assert overseer_plain.mytardis_setup == mytardis_setup
    Overseer.clear()


@responses.activate
def test_get_mytardis_setup_http_error(
    caplog: LogCaptureFixture,
    overseer: Overseer,
    connection: ConnectionConfig,
) -> None:
    responses.add(
        responses.GET,
        urljoin(connection.api_template, "introspection"),
        status=504,
    )

    caplog.set_level(logging.ERROR)
    error_str = "Introspection returned error:"
    with pytest.raises(HTTPError):
        _ = overseer.get_mytardis_setup()
        assert error_str in caplog.text
    Overseer.clear()


@mock.patch("src.overseers.overseer.Overseer.get_mytardis_setup")
def test_get_mytardis_setup_general_error(
    mock_get_mytardis_setup: Any,
    caplog: LogCaptureFixture,
    overseer: Overseer,
) -> None:
    mock_get_mytardis_setup.side_effect = IOError()
    error_str = "Non-HTTP exception in ConfigFromEnv.get_mytardis_setup"
    with pytest.raises(IOError):
        _ = overseer.get_mytardis_setup()
        assert error_str in caplog.text
    Overseer.clear()


@responses.activate
def test_get_mytardis_setup_no_objects(
    caplog: LogCaptureFixture,
    overseer: Overseer,
    connection: ConnectionConfig,
    response_dict_not_found: dict[str, Any],
) -> None:
    responses.add(
        responses.GET,
        urljoin(
            connection.api_template,
            "introspection",
        ),
        json=(response_dict_not_found),
        status=200,
    )
    caplog.set_level(logging.ERROR)
    error_str = (
        "MyTardis introspection did not return any data when called from "
        "ConfigFromEnv.get_mytardis_setup"
    )
    with pytest.raises(ValueError, match=error_str):
        _ = overseer.get_mytardis_setup()
        assert error_str in caplog.text
    Overseer.clear()


@responses.activate
def test_get_mytardis_setup_too_many_objects(
    caplog: LogCaptureFixture,
    overseer: Overseer,
    connection: ConnectionConfig,
    introspection_response_dict: dict[str, Any],
) -> None:
    test_dict = introspection_response_dict
    test_dict["objects"].append("Some Fake Data")
    responses.add(
        responses.GET,
        urljoin(
            connection.api_template,
            "introspection",
        ),
        json=(test_dict),
        status=200,
    )
    caplog.set_level(logging.ERROR)
    log_error_str = f"""MyTardis introspection returned more than one object when called from
        ConfigFromEnv.get_mytardis_setup\n
        Returned response was: {test_dict}"""
    error_str = (
        "MyTardis introspection returned more than one object when called from "
        "ConfigFromEnv.get_mytardis_setup"
    )

    with pytest.raises(ValueError, match=error_str):
        _ = overseer.get_mytardis_setup()
        assert log_error_str in caplog.text
    Overseer.clear()
