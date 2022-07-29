# Old style - at some point in the future could be refactored to use the fixtures

import logging

import pytest
import responses

from src.forges import Forge
from src.helpers.config import AuthConfig, ConnectionConfig

logger = logging.getLogger(__name__)
logger.propagate = True

test_object_name = "Test Project"

test_object_type = "project"

test_object_id = 1


@responses.activate
def test_post_returns_expected_tuple(
    caplog,
    auth: AuthConfig,
    connection: ConnectionConfig,
    project_object_dictionary,
    project_creation_response_dict,
):
    caplog.set_level(logging.INFO)
    responses.add(
        responses.POST,
        f"https://test.mytardis.nectar.auckland.ac.nz/api/v1/{test_object_type}/",
        status=200,
        json=(project_creation_response_dict),
    )
    forge = Forge(auth, connection)
    test_value = forge.forge_object(
        test_object_name,
        test_object_type,
        project_object_dictionary,
    )
    info_str = (
        f"Object: {test_object_name} successfully created in MyTardis\n"
        f"Object Type: {test_object_type}"
    )
    assert test_value == (test_object_name, True, "/api/v1/project/1/")
    assert info_str in caplog.text


@responses.activate
def test_post_returns_expected_tuple_when_no_json_return(
    caplog,
    auth: AuthConfig,
    connection: ConnectionConfig,
    project_object_dictionary,
):
    caplog.set_level(logging.INFO)
    responses.add(
        responses.POST,
        f"https://test.mytardis.nectar.auckland.ac.nz/api/v1/{test_object_type}/",
        status=201,
    )
    forge = Forge(auth, connection)
    test_value = forge.forge_object(
        test_object_name, test_object_type, project_object_dictionary
    )
    info_str = (
        f"Object: {test_object_name} successfully created in MyTardis\n"
        f"Object Type: {test_object_type}"
    )
    assert test_value == (test_object_name, True, None)
    assert info_str in caplog.text


@responses.activate
def test_overwrite_updates_action(
    caplog,
    auth: AuthConfig,
    connection: ConnectionConfig,
    project_object_dictionary,
    project_creation_response_dict,
):
    caplog.set_level(logging.INFO)
    # Catch any accidental POSTs - will cause test to fail if POST not PUT
    responses.add(
        responses.POST,
        f"https://test.mytardis.nectar.auckland.ac.nz/api/v1/{test_object_type}/",
        status=404,
    )
    responses.add(
        responses.PUT,
        f"https://test.mytardis.nectar.auckland.ac.nz/api/v1/{test_object_type}/1/",
        status=200,
        json=(project_creation_response_dict),
    )
    forge = Forge(auth, connection)
    test_value = forge.forge_object(
        test_object_name,
        test_object_type,
        project_object_dictionary,
        object_id=1,
        overwrite_objects=True,
    )
    info_str = (
        f"Object: {test_object_name} successfully created in MyTardis\n"
        f"Object Type: {test_object_type}"
    )
    assert test_value == (test_object_name, True, "/api/v1/project/1/")
    assert info_str in caplog.text


def test_overwrite_without_object_id_logs_warning(
    caplog,
    auth: AuthConfig,
    connection: ConnectionConfig,
    project_object_dictionary,
):
    caplog.set_level(logging.WARNING)
    forge = Forge(auth, connection)
    forge.forge_object(
        test_object_name,
        test_object_type,
        project_object_dictionary,
        overwrite_objects=True,
    )
    warning_str = (
        f"Overwrite was requested for an object of type {test_object_type} "
        "called from Forge class. There was no object_id passed with this request"
    )
    assert warning_str in caplog.text


@pytest.mark.dependency()
@responses.activate
def test_HTTPError_logs_warning(
    caplog,
    auth: AuthConfig,
    connection: ConnectionConfig,
    project_object_dictionary,
):
    responses.add(
        responses.POST,
        f"https://test.mytardis.nectar.auckland.ac.nz/api/v1/{test_object_type}/",
        status=504,
    )
    test_object_json = dict_to_json(project_object_dictionary)
    caplog.set_level(logging.WARNING)
    forge = Forge(auth, connection)
    test_value = forge.forge_object(
        test_object_name, test_object_type, project_object_dictionary
    )
    warning_str = (
        "Failed HTTP request from forge_object call\n"
        f"object_type: {test_object_type}\n"
        f"object_dict: {project_object_dictionary}\n"
        f"object_json: {test_object_json}"
    )
    assert warning_str in caplog.text
    assert test_value == (test_object_name, False)


@pytest.mark.dependency(depends=["test_HTTPError_logs_warning"])
@responses.activate
def test_HTTPError_fully_logs_error_at_error(
    caplog,
    auth: AuthConfig,
    connection: ConnectionConfig,
    project_object_dictionary,
):
    responses.add(
        responses.POST,
        f"https://test.mytardis.nectar.auckland.ac.nz/api/v1/{test_object_type}/",
        status=504,
    )
    caplog.set_level(logging.ERROR)
    forge = Forge(auth, connection)
    _ = forge.forge_object(
        test_object_name, test_object_type, project_object_dictionary
    )
    info_str = (
        "504 Server Error: Gateway Timeout for url: "
        "https://test.mytardis.nectar.auckland.ac.nz/api/v1/project/"
    )
    assert info_str in caplog.text


def side_effect_raise_value_error(action, url, **kwargs):
    raise ValueError


def test_non_HTTPError_logs_error(
    caplog,
    auth: AuthConfig,
    connection: ConnectionConfig,
    project_object_dictionary,
):
    caplog.set_level(logging.WARNING)
    forge = Forge(auth, connection)
    test_object_json = dict_to_json(project_object_dictionary)
    forge.rest_factory.mytardis_api_request = side_effect_raise_value_error
    warning_str = (
        "Non-HTTP request from forge_object call\n"
        f"object_type: {test_object_type}\n"
        f"object_dict: {project_object_dictionary}\n"
        f"object_json: {test_object_json}"
    )
    with pytest.raises(ValueError):
        _ = forge.forge_object(
            test_object_name, test_object_type, project_object_dictionary
        )
        assert warning_str in caplog.text
        assert "ValueError" in caplog.text


@pytest.mark.parametrize("status_code", [300, 301, 302])
@responses.activate
def test_response_status_larger_than_300_logs_error(
    caplog,
    status_code,
    auth: AuthConfig,
    connection: ConnectionConfig,
    project_object_dictionary,
):
    responses.add(
        responses.POST,
        f"https://test.mytardis.nectar.auckland.ac.nz/api/v1/{test_object_type}/",
        status=status_code,
    )
    caplog.set_level(logging.WARNING)
    forge = Forge(auth, connection)
    test_value = forge.forge_object(
        test_object_name, test_object_type, project_object_dictionary
    )
    warning_str = (
        "Object not successfully created in forge_object call\n"
        f"object_type: {test_object_type}\n"
        f"object_dict: {project_object_dictionary}\n"
        f"response status code: {status_code}"
    )
    assert warning_str in caplog.text
    assert test_value == (test_object_name, False)


@responses.activate
def test_no_uri_returns_warning(
    caplog,
    auth: AuthConfig,
    connection: ConnectionConfig,
    project_object_dictionary,
    project_creation_response_dict,
):

    test_response_dict_without_uri = project_creation_response_dict
    test_response_dict_without_uri.pop("resource_uri")
    responses.add(
        responses.POST,
        f"https://test.mytardis.nectar.auckland.ac.nz/api/v1/{test_object_type}/",
        status=200,
        json=test_response_dict_without_uri,
    )
    caplog.set_level(logging.WARNING)
    forge = Forge(auth, connection)
    test_value = forge.forge_object(
        test_object_name, test_object_type, project_object_dictionary
    )
    warning_str = (
        "No URI found for newly created object in forge_object call\n"
        f"object_type: {test_object_type}\n"
        f"object_dict: {project_object_dictionary}\n"
        f"response status code: 200\n"
        f"response text: {test_response_dict_without_uri}"
    )
    assert test_value == (test_object_name, False)
    assert warning_str in caplog.text
