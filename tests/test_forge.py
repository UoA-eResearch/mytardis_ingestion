# Old style - at some point in the future could be refactored to use the fixtures

import logging
from urllib.parse import urljoin

import pytest
import responses

from src.blueprints.custom_data_types import URI
from src.blueprints.project import Project
from src.config.config import ConnectionConfig
from src.forges import Forge
from src.helpers.enumerators import ObjectPostEnum

logger = logging.getLogger(__name__)
logger.propagate = True

test_object_name = "Test Project"

test_object_type = "project"

test_object_id = 1


@responses.activate
def test_post_returns_URI_on_success(
    caplog,
    project_uri: URI,
    connection: ConnectionConfig,
    forge: Forge,
    project: Project,
    project_creation_response_dict,
):
    caplog.set_level(logging.INFO)
    responses.add(
        responses.POST,
        urljoin(connection.api_template, test_object_type),
        status=200,
        json=(project_creation_response_dict),
    )
    test_value = forge.forge_object(project, test_object_id)

    info_str = (
        f"Object: {project.name} successfully "
        "created in MyTardis\n"
        f"Url substring: {ObjectPostEnum.PROJECT.value['url_substring']}"
    )
    assert test_value == project_uri
    assert info_str in caplog.text


@responses.activate
def test_post_returns_none_on_missing_body(
    caplog,
    connection: ConnectionConfig,
    forge: Forge,
    project: Project,
):
    caplog.set_level(logging.INFO)
    responses.add(
        responses.POST,
        urljoin(connection.api_template, test_object_type),
        status=201,
    )
    test_value = forge.forge_object(project, test_object_id)

    info_str = (
        f"Expected a JSON return from the POST request "
        "but no return was found. The object may not have "
        "been properly created and needs investigating.\n"
        f"Object in question is: {project}"
    )
    assert test_value is None
    assert info_str in caplog.text


# TODO I don't understand what this test is doing
@responses.activate
@pytest.mark.skip(reason="What does this test do?")
def test_overwrite_updates_action(
    caplog,
    project_uri: URI,
    connection: ConnectionConfig,
    forge: Forge,
    project: Project,
    project_creation_response_dict,
):
    caplog.set_level(logging.INFO)
    url = urljoin(connection.api_template, test_object_type)
    # Catch any accidental POSTs - will cause test to fail if POST not PUT
    responses.add(responses.POST, url, status=404)
    responses.add(
        responses.PUT,
        urljoin(url, "1/"),
        status=200,
        json=(project_creation_response_dict),
    )
    test_value = forge.forge_object(project, test_object_id, True)
    info_str = (
        f"Object: {test_object_name} successfully created in MyTardis\n"
        f"Object Type: {test_object_type}"
    )
    assert test_value == project_uri
    assert info_str in caplog.text


def test_overwrite_without_object_id_logs_warning(
    caplog,
    forge: Forge,
    project: Project,
):
    caplog.set_level(logging.WARNING)
    forge.forge_object(project, overwrite_objects=True)

    warning_str = (
        f"Overwrite was requested for an object of type {ObjectPostEnum.PROJECT.value} "
        "called from Forge class. There was no object_id passed with this request"
    )
    assert warning_str in caplog.text


@pytest.mark.dependency()
@responses.activate
def test_HTTPError_logs_warning(
    caplog,
    connection: ConnectionConfig,
    forge: Forge,
    project: Project,
):
    url = urljoin(connection.api_template, test_object_type)
    responses.add(
        responses.POST,
        url,
        status=504,
    )
    caplog.set_level(logging.WARNING)
    test_value = forge.forge_object(project, test_object_type)

    warning_str = (
        "Failed HTTP request from forge_object call\n"
        f"Url: {url}\nAction: {responses.POST}\nData: {project.json(exclude_none=True)}"
    )
    assert warning_str in caplog.text
    assert test_value is None


@pytest.mark.dependency(depends=["test_HTTPError_logs_warning"])
@responses.activate
def test_HTTPError_fully_logs_error_at_error(
    caplog,
    connection: ConnectionConfig,
    forge: Forge,
    project: Project,
):
    url = urljoin(connection.api_template, test_object_type)
    responses.add(
        responses.POST,
        url,
        status=504,
    )
    caplog.set_level(logging.ERROR)
    _ = forge.forge_object(project, test_object_type)
    info_str = "504 Server Error: Gateway Timeout for url: " f"{url}"
    assert info_str in caplog.text


def side_effect_raise_value_error(action, url, **kwargs):
    raise ValueError


def test_non_HTTPError_logs_error(
    caplog,
    connection: ConnectionConfig,
    forge: Forge,
    project: Project,
):
    url = urljoin(connection.api_template, test_object_type)
    caplog.set_level(logging.WARNING)
    forge.rest_factory.mytardis_api_request = side_effect_raise_value_error
    warning_str = (
        "Non-HTTP request from forge_object call\n"
        f"Url: {url}\nAction: POST"
        f"\nData: {project.json(exclude_none=True)}"
    )
    with pytest.raises(ValueError):
        _ = forge.forge_object(project, test_object_type)

    assert warning_str in caplog.text
    assert "ValueError" in caplog.text


@pytest.mark.parametrize("status_code", [300, 301, 302])
@responses.activate
def test_response_status_larger_than_300_logs_error(
    caplog,
    connection: ConnectionConfig,
    status_code: int,
    forge: Forge,
    project: Project,
):
    url = urljoin(connection.api_template, test_object_type)
    responses.add(
        responses.POST,
        url,
        status=status_code,
    )
    caplog.set_level(logging.WARNING)
    test_value = forge.forge_object(project, test_object_type)
    warning_str = (
        "Object not successfully created in forge_object call\n"
        f"Url: {url}\nAction: {responses.POST}\nData: {project.json(exclude_none=True)}"
        f"response status code: {status_code}"
    )
    assert warning_str in caplog.text
    assert test_value is None


@responses.activate
def test_no_uri_returns_warning(
    caplog,
    connection: ConnectionConfig,
    forge: Forge,
    project: Project,
    project_creation_response_dict,
):
    test_response_dict_without_uri = project_creation_response_dict
    test_response_dict_without_uri.pop("resource_uri")
    responses.add(
        responses.POST,
        urljoin(connection.api_template, test_object_type),
        status=200,
        json=test_response_dict_without_uri,
    )
    caplog.set_level(logging.WARNING)
    test_value = forge.forge_object(project, test_object_type)
    warning_str = (
        "No URI was able to be discerned when creating object: "
        f"{project.name}. Object may have "
        "been successfully created in MyTardis, but needs further "
        "investigation."
    )

    assert test_value is None
    assert warning_str in caplog.text
