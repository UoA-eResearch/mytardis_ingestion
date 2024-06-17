# Old style - at some point in the future could be refactored to use the fixtures
# pylint: disable=missing-function-docstring,missing-module-docstring
# nosec assert_used
# flake8: noqa S101

import logging
from typing import Any, Dict
from urllib.parse import urljoin

import mock
import pytest
import responses
from pytest import LogCaptureFixture

from src.blueprints.project import Project
from src.config.config import ConnectionConfig
from src.forges.forge import Forge
from src.mytardis_client.data_types import URI
from src.mytardis_client.enumerators import ObjectPostEnum

logger = logging.getLogger(__name__)
logger.propagate = True

TEST_OBJECT_NAME = "Test Project"

TEST_OBJECT_TYPE = "project"

TEST_OBJECT_ID = 1


@pytest.mark.xfail
@responses.activate
def test_post_returns_URI_on_success(  # pylint: disable=invalid-name
    caplog: LogCaptureFixture,
    project_uri: URI,
    connection: ConnectionConfig,
    forge: Forge,
    project: Project,
    project_creation_response_dict: Dict[str, Any],
) -> None:
    caplog.set_level(logging.INFO)
    responses.add(
        responses.POST,
        urljoin(connection.api_template, TEST_OBJECT_TYPE),
        status=200,
        json=(project_creation_response_dict),
    )
    test_value = forge.forge_object(project)

    info_str = (
        f"Object: {project.name} successfully "
        "created in MyTardis\n"
        f"Url substring: {ObjectPostEnum.PROJECT.value['url_substring']}"
    )
    assert test_value == project_uri
    assert info_str in caplog.text


@pytest.mark.xfail
@responses.activate
def test_post_returns_none_on_missing_body(
    caplog: LogCaptureFixture,
    connection: ConnectionConfig,
    forge: Forge,
    project: Project,
) -> None:
    caplog.set_level(logging.INFO)
    responses.add(
        responses.POST,
        urljoin(connection.api_template, TEST_OBJECT_TYPE),
        status=201,
    )
    test_value = forge.forge_object(project)

    info_str = (
        f"Expected a JSON return from the POST request "
        "but no return was found. The object may not have "
        "been properly created and needs investigating.\n"
        f"Object in question is: {project}"
    )
    assert test_value is None
    assert info_str in caplog.text


@pytest.mark.xfail
@pytest.mark.dependency()
@responses.activate
def test_HTTPError_logs_warning(  # pylint: disable=invalid-name
    caplog: LogCaptureFixture,
    connection: ConnectionConfig,
    forge: Forge,
    project: Project,
) -> None:
    url = urljoin(connection.api_template, TEST_OBJECT_TYPE)
    responses.add(
        responses.POST,
        url,
        status=504,
    )
    caplog.set_level(logging.WARNING)
    test_value = forge.forge_object(project)

    warning_str = (
        "Failed HTTP request from forge_object call\n"
        f"Url: {url}\nAction: {responses.POST}\nData: {project.json(exclude_none=True)}"
    )
    assert warning_str in caplog.text
    assert test_value is None


@pytest.mark.xfail
@pytest.mark.dependency(depends=["test_HTTPError_logs_warning"])
@responses.activate
def test_HTTPError_fully_logs_error_at_error(  # pylint: disable=invalid-name
    caplog: LogCaptureFixture,
    connection: ConnectionConfig,
    forge: Forge,
    project: Project,
) -> None:
    url = urljoin(connection.api_template, TEST_OBJECT_TYPE)
    responses.add(
        responses.POST,
        url,
        status=504,
    )
    caplog.set_level(logging.ERROR)
    _ = forge.forge_object(project)
    info_str = "504 Server Error: Gateway Timeout for url: " f"{url}"
    assert info_str in caplog.text


@pytest.mark.xfail
@mock.patch("src.mytardis_client.mt_rest.MyTardisRESTFactory.mytardis_api_request")
def test_non_HTTPError_logs_error(  # pylint: disable=invalid-name
    mock_mytardis_api_request: Any,
    caplog: LogCaptureFixture,
    connection: ConnectionConfig,
    forge: Forge,
    project: Project,
) -> None:
    url = urljoin(connection.api_template, TEST_OBJECT_TYPE)
    caplog.set_level(logging.WARNING)
    mock_mytardis_api_request.side_effect = ValueError()
    warning_str = (
        "Non-HTTP request from forge_object call\n"
        f"Url: {url}\nAction: POST"
        f"\nData: {project.json(exclude_none=True)}"
    )
    with pytest.raises(ValueError):
        _ = forge.forge_object(project)

    assert warning_str in caplog.text
    assert "ValueError" in caplog.text


@pytest.mark.xfail
@pytest.mark.parametrize("status_code", [300, 301, 302])
@responses.activate
def test_response_status_larger_than_300_logs_error(
    caplog: Any,
    connection: ConnectionConfig,
    status_code: int,
    forge: Forge,
    project: Project,
) -> None:
    url = urljoin(connection.api_template, TEST_OBJECT_TYPE)
    responses.add(
        responses.POST,
        url,
        status=status_code,
    )
    caplog.set_level(logging.WARNING)
    test_value = forge.forge_object(project)
    warning_str = (
        "Object not successfully created in forge_object call\n"
        f"Url: {url}\nAction: {responses.POST}\nData: {project.json(exclude_none=True)}"
        f"response status code: {status_code}"
    )
    assert warning_str in caplog.text
    assert test_value is None


@pytest.mark.xfail
@responses.activate
def test_no_uri_returns_warning(
    caplog: LogCaptureFixture,
    connection: ConnectionConfig,
    forge: Forge,
    project: Project,
    project_creation_response_dict: Dict[str, Any],
) -> None:
    test_response_dict_without_uri = project_creation_response_dict
    test_response_dict_without_uri.pop("resource_uri")
    responses.add(
        responses.POST,
        urljoin(connection.api_template, TEST_OBJECT_TYPE),
        status=200,
        json=test_response_dict_without_uri,
    )
    caplog.set_level(logging.WARNING)
    test_value = forge.forge_object(project)
    warning_str = (
        "No URI was able to be discerned when creating object: "
        f"{project.name}. Object may have "
        "been successfully created in MyTardis, but needs further "
        "investigation."
    )

    assert test_value is None
    assert warning_str in caplog.text
