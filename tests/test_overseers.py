# pylint: disable=missing-function-docstring

"""Tests of the Overseer class and its functions"""
import logging
from urllib.parse import urljoin

import mock
import pytest
import responses
from pytest import fixture
from responses import matchers
from src.helpers.config import MyTardisAuth, MyTardisConnection, MyTardisIntrospection

from src.overseers import Overseer


logger = logging.getLogger(__name__)
logger.propagate = True


@fixture
@responses.activate
def overseer(
    auth: MyTardisAuth,
    connection: MyTardisConnection,
    mytardis_setup: MyTardisIntrospection,
) -> Overseer:
    return Overseer(auth, connection, mytardis_setup)


def test_staticmethod_resource_uri_to_id():
    test_uri = "/api/v1/user/10/"
    assert Overseer.resource_uri_to_id(test_uri) == 10


@responses.activate
def test_get_objects(
    connection: MyTardisConnection,
    overseer: Overseer,
    project_response_dict,
):
    object_type = "project"
    search_target = "pids"
    search_string = "Project_1"
    responses.add(
        responses.GET,
        urljoin(connection.api_template, object_type),
        json=(project_response_dict),
        match=[
            matchers.query_param_matcher(
                {search_target: search_string},
            ),
        ],
        status=200,
    )

    assert (
        overseer.get_objects(
            object_type,
            search_target,
            search_string,
        )
        == project_response_dict["objects"]
    )


@responses.activate
def test_get_objects_http_error(
    caplog,
    connection: MyTardisConnection,
    overseer: Overseer,
):
    object_type = "project"
    search_target = "pids"
    search_string = "Project_1"
    responses.add(
        responses.GET,
        urljoin(connection.api_template, object_type),
        match=[
            matchers.query_param_matcher(
                {search_target: search_string},
            ),
        ],
        status=504,
    )
    caplog.set_level(logging.WARNING)
    warning_str = (
        "Failed HTTP request from Overseer.get_objects call\n"
        f"object_type = {object_type}\n"
        f"search_target = {search_target}\n"
        f"search_string = {search_string}"
    )
    assert (
        overseer.get_objects(
            object_type,
            search_target,
            search_string,
        )
        is None
    )
    assert warning_str in caplog.text


@mock.patch("src.helpers.mt_rest.MyTardisRESTFactory.mytardis_api_request")
def test_get_objects_general_error(
    mock_mytardis_api_request,
    caplog,
    overseer: Overseer,
):
    mock_mytardis_api_request.side_effect = IOError()
    object_type = "project"
    search_target = "pids"
    search_string = "Project_1"
    error_str = (
        "Non-HTTP exception in Overseer.get_objects call\n"
        f"object_type = {object_type}\n"
        f"search_target = {search_target}\n"
        f"search_string = {search_string}"
    )
    with pytest.raises(IOError):
        _ = overseer.get_objects(
            object_type,
            search_target,
            search_string,
        )
        assert error_str in caplog.text


@responses.activate
def test_get_objects_no_objects(
    connection: MyTardisConnection,
    overseer: Overseer,
    response_dict_not_found,
):
    object_type = "project"
    search_target = "pids"
    search_string = "Project_1"
    responses.add(
        responses.GET,
        urljoin(connection.api_template, object_type),
        json=(response_dict_not_found),
        match=[
            matchers.query_param_matcher(
                {search_target: search_string},
            ),
        ],
        status=200,
    )

    assert (
        overseer.get_objects(
            object_type,
            search_target,
            search_string,
        )
        is None
    )


@responses.activate
def test_get_uris(
    connection: MyTardisConnection,
    overseer: Overseer,
    project_response_dict,
):
    object_type = "project"
    search_target = "pids"
    search_string = "Project_1"
    responses.add(
        responses.GET,
        urljoin(connection.api_template, object_type),
        json=(project_response_dict),
        match=[
            matchers.query_param_matcher(
                {search_target: search_string},
            ),
        ],
        status=200,
    )
    assert overseer.get_uris(
        object_type,
        search_target,
        search_string,
    ) == ["/api/v1/project/1/"]


@responses.activate
def test_get_uris_no_objects(
    connection: MyTardisConnection,
    overseer: Overseer,
    response_dict_not_found,
):
    object_type = "project"
    search_target = "pids"
    search_string = "Project_1"
    responses.add(
        responses.GET,
        urljoin(connection.api_template, object_type),
        json=(response_dict_not_found),
        match=[
            matchers.query_param_matcher(
                {search_target: search_string},
            ),
        ],
        status=200,
    )
    assert (
        overseer.get_uris(
            object_type,
            search_target,
            search_string,
        )
        is None
    )


@responses.activate
def test_get_uris_malformed_return_dict(
    caplog,
    connection: MyTardisConnection,
    overseer: Overseer,
    project_response_dict,
):
    caplog.set_level(logging.ERROR)
    test_dict = project_response_dict
    test_dict["objects"][0].pop("resource_uri")
    object_type = "project"
    search_target = "pids"
    search_string = "Project_1"
    responses.add(
        responses.GET,
        urljoin(connection.api_template, object_type),
        json=(project_response_dict),
        match=[
            matchers.query_param_matcher(
                {search_target: search_string},
            ),
        ],
        status=200,
    )
    error_str = (
        "Malformed return from MyTardis. No resource_uri found for "
        f"{object_type} searching on {search_target} with {search_string}"
    )
    with pytest.raises(KeyError):
        _ = overseer.get_uris(
            object_type,
            search_target,
            search_string,
        )
        assert error_str in caplog.text


@responses.activate
def test_get_uris_ensure_http_errors_caught_by_get_objects(
    caplog,
    connection: MyTardisConnection,
    overseer: Overseer,
):
    caplog.set_level(logging.WARNING)
    object_type = "project"
    search_target = "pids"
    search_string = "Project_1"
    responses.add(
        responses.GET,
        urljoin(connection.api_template, object_type),
        match=[
            matchers.query_param_matcher(
                {search_target: search_string},
            ),
        ],
        status=504,
    )
    warning_str = (
        "Failed HTTP request from Overseer.get_objects call\n"
        f"object_type = {object_type}\n"
        f"search_target = {search_target}\n"
        f"search_string = {search_string}"
    )
    assert (
        overseer.get_uris(
            object_type,
            search_target,
            search_string,
        )
        is None
    )
    assert warning_str in caplog.text


@mock.patch("src.helpers.mt_rest.MyTardisRESTFactory.mytardis_api_request")
def test_get_uris_general_error(
    mock_mytardis_api_request,
    overseer: Overseer,
):
    mock_mytardis_api_request.side_effect = IOError()
    object_type = "project"
    search_target = "pids"
    search_string = "Project_1"
    with pytest.raises(IOError):
        _ = overseer.get_uris(
            object_type,
            search_target,
            search_string,
        )


def test_is_uri_true():
    object_type = "project"
    uri_string = f"/api/v1/{object_type}/1/"
    assert Overseer.is_uri(uri_string, object_type)


def test_is_uri_false_text_before_uri():
    object_type = "project"
    uri_string = f"http://test.com/api/v1/{object_type}/1/"
    assert Overseer.is_uri(uri_string, object_type) is False


def test_is_uri_false_text_after_uri():
    object_type = "project"
    uri_string = f"/api/v1/{object_type}/1/edit/"
    assert Overseer.is_uri(uri_string, object_type) is False


def test_is_uri_false():
    object_type = "project"
    uri_string = "Some other text"
    assert Overseer.is_uri(uri_string, object_type) is False


def test_is_uri_int_not_string():
    object_type = "project"
    uri_string = 1
    assert Overseer.is_uri(uri_string, object_type) is False


@responses.activate
def test_get_uris_by_identifier(
    connection: MyTardisConnection,
    overseer: Overseer,
    project_response_dict,
):
    object_type = "project"
    search_target = "pids"
    search_string = "Project_1"
    responses.add(
        responses.GET,
        urljoin(connection.api_template, object_type),
        json=(project_response_dict),
        match=[
            matchers.query_param_matcher(
                {search_target: search_string},
            ),
        ],
        status=200,
    )
    assert overseer.get_uris_by_identifier(
        object_type,
        search_string,
    ) == ["/api/v1/project/1/"]


def test_get_uris_by_identifier_app_not_used(
    caplog,
    overseer: Overseer,
):
    caplog.set_level(logging.WARNING)
    object_type = "project"
    search_string = "Project_1"
    overseer.mytardis_setup.objects_with_ids = []
    warning_str = (
        "The identifiers app is not installed in the instance of MyTardis, "
        "or there are no objects defined in OBJECTS_WITH_IDENTIFIERS in "
        "settings.py"
    )
    assert overseer.get_uris_by_identifier(object_type, search_string) is None
    assert warning_str in caplog.text


def test_get_uris_by_identifier_object_not_set_up_for_ids(
    caplog,
    overseer: Overseer,
):
    caplog.set_level(logging.WARNING)
    object_type = "project"
    search_string = "Project_1"
    overseer.mytardis_setup.objects_with_ids = ["experiment"]
    warning_str = (
        f"The object type, {object_type}, is not present in "
        "OBJECTS_WITH_IDENTIFIERS defined in settings.py"
    )
    assert overseer.get_uris_by_identifier(object_type, search_string) is None
    assert warning_str in caplog.text
