# pylint: disable=missing-function-docstring

"""Tests of the Overseer class and its functions"""
import logging
from typing import Any, Dict
from urllib.parse import urljoin

import mock
import pytest
import responses
from pytest import LogCaptureFixture
from requests import HTTPError
from responses import matchers

from src.blueprints.custom_data_types import URI
from src.blueprints.storage_boxes import StorageBox
from src.config.config import ConnectionConfig, IntrospectionConfig
from src.helpers.enumerators import ObjectSearchEnum
from src.helpers.mt_rest import MyTardisRESTFactory
from src.overseers import Overseer

logger = logging.getLogger(__name__)
logger.propagate = True


@pytest.fixture(name="overseer_plain")
def fixture_overseer_plain(
    rest_factory: MyTardisRESTFactory,
) -> Any:
    return Overseer(rest_factory)


def test_staticmethod_resource_uri_to_id() -> None:
    test_uri = URI("/v1/user/10/")
    assert Overseer.resource_uri_to_id(test_uri) == 10


@responses.activate
def test_get_objects(
    overseer: Overseer,
    connection: ConnectionConfig,
    project_response_dict: dict[str, Any],
) -> None:
    object_type = ObjectSearchEnum.PROJECT.value
    search_string = "Project_1"
    responses.add(
        responses.GET,
        urljoin(connection.api_template, object_type["type"]),
        json=(project_response_dict),
        match=[
            matchers.query_param_matcher(
                {object_type["target"]: search_string},
            ),
        ],
        status=200,
    )
    responses.add(
        responses.GET,
        urljoin(connection.api_template, object_type["type"]),
        json=(project_response_dict),
        match=[
            matchers.query_param_matcher(
                {"identifiers": search_string},
            ),
        ],
        status=200,
    )
    assert (
        overseer.get_objects(
            object_type,
            search_string,
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
    object_type = ObjectSearchEnum.PROJECT.value
    search_string = "Project_1"
    responses.add(
        responses.GET,
        urljoin(connection.api_template, object_type["type"]),
        match=[
            matchers.query_param_matcher(
                {object_type["target"]: search_string},
            ),
        ],
        status=504,
    )
    responses.add(
        responses.GET,
        urljoin(connection.api_template, object_type["type"]),
        match=[
            matchers.query_param_matcher(
                {"identifiers": search_string},
            ),
        ],
        status=504,
    )
    caplog.set_level(logging.WARNING)
    warning_str = (
        "Failed HTTP request from Overseer.get_objects call\n"
        f"object_type = {object_type}\n"
        "query_params"
    )
    assert (
        overseer.get_objects(
            object_type,
            search_string,
        )
        == []
    )
    assert warning_str in caplog.text
    Overseer.clear()


@mock.patch("src.helpers.mt_rest.MyTardisRESTFactory.mytardis_api_request")
def test_get_objects_general_error(
    mock_mytardis_api_request: Any,
    caplog: LogCaptureFixture,
    overseer: Overseer,
) -> None:
    mock_mytardis_api_request.side_effect = IOError()
    object_type = ObjectSearchEnum.PROJECT.value
    search_string = "Project_1"
    error_str = (
        "Non-HTTP exception in Overseer.get_objects call\n"
        f"object_type = {object_type}\n"
        "query_params"
    )
    with pytest.raises(IOError):
        _ = overseer.get_objects(
            object_type,
            search_string,
        )
        assert error_str in caplog.text
    Overseer.clear()


@responses.activate
def test_get_objects_no_objects(
    overseer: Overseer,
    connection: ConnectionConfig,
    response_dict_not_found: dict[str, Any],
) -> None:
    object_type = ObjectSearchEnum.PROJECT.value
    search_string = "Project_1"
    responses.add(
        responses.GET,
        urljoin(connection.api_template, object_type["type"]),
        json=(response_dict_not_found),
        match=[
            matchers.query_param_matcher(
                {object_type["target"]: search_string},
            ),
        ],
        status=200,
    )
    responses.add(
        responses.GET,
        urljoin(connection.api_template, object_type["type"]),
        json=(response_dict_not_found),
        match=[
            matchers.query_param_matcher(
                {"identifiers": search_string},
            ),
        ],
        status=200,
    )

    assert (
        overseer.get_objects(
            object_type,
            search_string,
        )
        == []
    )
    Overseer.clear()


@responses.activate
def test_get_uris(
    connection: ConnectionConfig,
    overseer: Overseer,
    project_response_dict: dict[str, Any],
) -> None:
    object_type = ObjectSearchEnum.PROJECT.value
    search_string = "Project_1"
    responses.add(
        responses.GET,
        urljoin(connection.api_template, object_type["type"]),
        json=(project_response_dict),
        match=[
            matchers.query_param_matcher(
                {object_type["target"]: search_string},
            ),
        ],
        status=200,
    )
    responses.add(
        responses.GET,
        urljoin(connection.api_template, object_type["type"]),
        json=(project_response_dict),
        match=[
            matchers.query_param_matcher(
                {"identifiers": search_string},
            ),
        ],
        status=200,
    )
    assert overseer.get_uris(
        object_type,
        search_string,
    ) == [URI("/api/v1/project/1/")]
    Overseer.clear()


@responses.activate
def test_get_uris_no_objects(
    connection: ConnectionConfig,
    overseer: Overseer,
    response_dict_not_found: dict[str, Any],
) -> None:
    object_type = ObjectSearchEnum.PROJECT.value
    search_string = "Project_1"
    responses.add(
        responses.GET,
        urljoin(connection.api_template, object_type["type"]),
        json=(response_dict_not_found),
        match=[
            matchers.query_param_matcher(
                {object_type["target"]: search_string},
            ),
        ],
        status=200,
    )
    responses.add(
        responses.GET,
        urljoin(connection.api_template, object_type["type"]),
        json=(response_dict_not_found),
        match=[
            matchers.query_param_matcher(
                {"identifiers": search_string},
            ),
        ],
        status=200,
    )
    assert (
        overseer.get_uris(
            object_type,
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
    object_type = ObjectSearchEnum.PROJECT.value
    search_string = "Project_1"
    responses.add(
        responses.GET,
        urljoin(connection.api_template, object_type["type"]),
        json=(test_dict),
        match=[
            matchers.query_param_matcher(
                {object_type["target"]: search_string},
            ),
        ],
        status=200,
    )
    responses.add(
        responses.GET,
        urljoin(connection.api_template, object_type["type"]),
        json=(test_dict),
        match=[
            matchers.query_param_matcher(
                {"identifiers": search_string},
            ),
        ],
        status=200,
    )
    error_str = (
        "Malformed return from MyTardis. No resource_uri found for "
        f"{object_type} searching with {search_string}"
    )
    with pytest.raises(KeyError):
        _ = overseer.get_uris(
            object_type,
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
    object_type = ObjectSearchEnum.PROJECT.value
    search_string = "Project_1"
    responses.add(
        responses.GET,
        urljoin(connection.api_template, object_type["type"]),
        match=[
            matchers.query_param_matcher(
                {object_type["target"]: search_string},
            ),
        ],
        status=504,
    )
    responses.add(
        responses.GET,
        urljoin(connection.api_template, object_type["type"]),
        match=[
            matchers.query_param_matcher(
                {"identifiers": search_string},
            ),
        ],
        status=504,
    )
    warning_str = (
        "Failed HTTP request from Overseer.get_objects call\n"
        f"object_type = {object_type}\n"
        "query_params"
    )
    assert (
        overseer.get_uris(
            object_type,
            search_string,
        )
        == []
    )
    assert warning_str in caplog.text
    Overseer.clear()


@mock.patch("src.helpers.mt_rest.MyTardisRESTFactory.mytardis_api_request")
def test_get_uris_general_error(
    mock_mytardis_api_request: Any,
    overseer: Overseer,
) -> None:
    mock_mytardis_api_request.side_effect = IOError()
    object_type = ObjectSearchEnum.PROJECT.value
    search_string = "Project_1"
    with pytest.raises(IOError):
        _ = overseer.get_uris(
            object_type,
            search_string,
        )
    Overseer.clear()


@responses.activate
def test_get_storagebox(
    storage_box_response_dict: Dict[str, str],
    overseer: Overseer,
    storage_box: StorageBox,
    storage_box_name: str,
    connection: ConnectionConfig,
) -> None:
    responses.add(
        responses.GET,
        urljoin(connection.api_template, ObjectSearchEnum.STORAGE_BOX.value["type"]),
        match=[
            matchers.query_param_matcher(
                {
                    "name": storage_box_name,
                },
            ),
        ],
        json=(storage_box_response_dict),
        status=200,
    )
    assert overseer.get_storage_box(storage_box_name) == storage_box
    Overseer.clear()


@responses.activate
def test_get_storagebox_no_storage_box_found(
    response_dict_not_found: dict[str, Any],
    caplog: LogCaptureFixture,
    overseer: Overseer,
    storage_box_name: str,
    connection: ConnectionConfig,
) -> None:
    caplog.set_level(logging.WARNING)
    warning_str = f"Unable to locate storage box called {storage_box_name}"
    responses.add(
        responses.GET,
        urljoin(connection.api_template, ObjectSearchEnum.STORAGE_BOX.value["type"]),
        match=[
            matchers.query_param_matcher(
                {
                    "name": storage_box_name,
                },
            ),
        ],
        json=(response_dict_not_found),
        status=200,
    )
    responses.add(
        responses.GET,
        urljoin(connection.api_template, ObjectSearchEnum.STORAGE_BOX.value["type"]),
        match=[
            matchers.query_param_matcher(
                {
                    "identifiers": storage_box_name,
                },
            ),
        ],
        json=(response_dict_not_found),
        status=200,
    )
    assert overseer.get_storage_box(storage_box_name) is None
    assert warning_str in caplog.text
    Overseer.clear()


@responses.activate
def test_get_storagebox_too_many_returns(
    storage_box_response_dict: Dict[str, Any],
    caplog: LogCaptureFixture,
    overseer: Overseer,
    storage_box_name: str,
    connection: ConnectionConfig,
) -> None:
    warning_str = (
        "Unable to uniquely identify the storage box based on the "
        f"name provided ({storage_box_name}). Please check with your "
        "system administrator to identify the source of the issue."
    )
    storage_box_response_dict["objects"].append({"name": "Test_box_2"})
    responses.add(
        responses.GET,
        urljoin(connection.api_template, ObjectSearchEnum.STORAGE_BOX.value["type"]),
        match=[
            matchers.query_param_matcher(
                {
                    "name": storage_box_name,
                },
            ),
        ],
        json=(storage_box_response_dict),
        status=200,
    )
    responses.add(
        responses.GET,
        urljoin(connection.api_template, ObjectSearchEnum.STORAGE_BOX.value["type"]),
        match=[
            matchers.query_param_matcher(
                {
                    "identifers": storage_box_name,
                },
            ),
        ],
        status=200,
    )
    caplog.set_level(logging.WARNING)
    print(overseer.get_storage_box(storage_box_name))
    assert overseer.get_storage_box(storage_box_name) is None
    assert warning_str in caplog.text
    Overseer.clear()


@responses.activate
def test_get_storagebox_no_name(
    storage_box_response_dict: Dict[str, Any],
    overseer: Overseer,
    storage_box_name: str,
    connection: ConnectionConfig,
    caplog: LogCaptureFixture,
) -> None:
    caplog.set_level(logging.WARNING)
    storage_box_response_dict["objects"][0].pop("name")
    warning_str = (
        f"Storage box, {storage_box_name} is misconfigured. Storage box "
        f"gathered from MyTardis:"
    )
    responses.add(
        responses.GET,
        urljoin(connection.api_template, ObjectSearchEnum.STORAGE_BOX.value["type"]),
        match=[
            matchers.query_param_matcher(
                {
                    "name": storage_box_name,
                },
            ),
        ],
        json=(storage_box_response_dict),
        status=200,
    )
    responses.add(
        responses.GET,
        urljoin(connection.api_template, ObjectSearchEnum.STORAGE_BOX.value["type"]),
        match=[
            matchers.query_param_matcher(
                {
                    "identifiers": storage_box_name,
                },
            ),
        ],
        json=(storage_box_response_dict),
        status=200,
    )
    assert overseer.get_storage_box(storage_box_name) is None
    assert warning_str in caplog.text
    Overseer.clear()


@responses.activate
def test_get_storagebox_no_location(
    storage_box_response_dict: Dict[str, Any],
    overseer: Overseer,
    storage_box_name: str,
    connection: ConnectionConfig,
    caplog: LogCaptureFixture,
) -> None:
    caplog.set_level(logging.WARNING)
    storage_box_response_dict["objects"][0]["options"] = [
        {"key": "not_location", "value": "Nothing"},
    ]
    warning_str = f"Storage box, {storage_box_name} is misconfigured. Missing location"
    responses.add(
        responses.GET,
        urljoin(connection.api_template, ObjectSearchEnum.STORAGE_BOX.value["type"]),
        match=[
            matchers.query_param_matcher(
                {
                    "name": storage_box_name,
                },
            ),
        ],
        json=(storage_box_response_dict),
        status=200,
    )
    responses.add(
        responses.GET,
        urljoin(connection.api_template, ObjectSearchEnum.STORAGE_BOX.value["type"]),
        match=[
            matchers.query_param_matcher(
                {
                    "identifiers": storage_box_name,
                },
            ),
        ],
        json=(storage_box_response_dict),
        status=200,
    )
    assert overseer.get_storage_box(storage_box_name) is None
    assert warning_str in caplog.text
    Overseer.clear()


@responses.activate
def test_get_storagebox_no_description(
    storage_box_response_dict: Dict[str, Any],
    overseer: Overseer,
    caplog: LogCaptureFixture,
    storage_box: StorageBox,
    storage_box_name: str,
    connection: ConnectionConfig,
) -> None:
    caplog.set_level(logging.WARNING)
    storage_box_response_dict["objects"][0].pop("description")
    warning_str = f"No description given for Storage Box, {storage_box_name}"
    responses.add(
        responses.GET,
        urljoin(connection.api_template, ObjectSearchEnum.STORAGE_BOX.value["type"]),
        match=[
            matchers.query_param_matcher(
                {
                    "name": storage_box_name,
                },
            ),
        ],
        json=(storage_box_response_dict),
        status=200,
    )
    responses.add(
        responses.GET,
        urljoin(connection.api_template, ObjectSearchEnum.STORAGE_BOX.value["type"]),
        match=[
            matchers.query_param_matcher(
                {
                    "identifiers": storage_box_name,
                },
            ),
        ],
        json=(storage_box_response_dict),
        status=200,
    )
    storage_box.description = "No description"
    assert overseer.get_storage_box(storage_box_name) == storage_box
    assert warning_str in caplog.text
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
