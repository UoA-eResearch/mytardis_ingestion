# pylint: disable=missing-function-docstring,redefined-outer-name,too-many-lines

"""Tests of the Overseer class and its functions"""
import logging

import mock
import pytest
import responses
from pytest import fixture
from requests.exceptions import HTTPError
from responses import matchers

from src.overseers import Overseer

from .conftest import (  # pylint: disable=unused-import
    config_dict,
    institution_response_dict,
    introspection_response_dict,
    mytardis_setup_dict,
    project_response_dict,
    response_dict_not_found,
)

# from requests import Response


logger = logging.getLogger(__name__)
logger.propagate = True


@fixture
@responses.activate
def overseer(config_dict, introspection_response_dict):
    responses.add(
        responses.GET,
        "https://test.mytardis.nectar.auckland.ac.nz/api/v1/introspection",
        json=(introspection_response_dict),
        status=200,
    )

    return Overseer(config_dict)


@fixture
def search_with_no_uri_dict():
    return {
        "institution": "Uni RoR",
        "project": "Test_Project",
        "experiment": "Test_Experiment",
        "dataset": "Test_Dataset",
        "instrument": "Instrument_1",
    }


def test_staticmethod_resource_uri_to_id():
    test_uri = "/api/v1/user/10/"
    assert Overseer.resource_uri_to_id(test_uri) == 10  # nosec


@responses.activate
def test_get_mytardis_setup(
    config_dict,
    overseer,
    introspection_response_dict,
    mytardis_setup_dict,
):
    responses.add(
        responses.GET,
        f"{config_dict['hostname']}/api/v1/introspection",
        json=(introspection_response_dict),
        status=200,
    )

    assert overseer.get_mytardis_set_up() == mytardis_setup_dict  # nosec


@responses.activate
def test_get_mytardis_setup_http_error(
    caplog,
    config_dict,
    overseer,
):
    responses.add(
        responses.GET,
        f"{config_dict['hostname']}/api/v1/introspection",
        status=504,
    )
    caplog.set_level(logging.ERROR)
    error_str = "Failed HTTP request from Overseer.get_mytardis_set_up"
    with pytest.raises(HTTPError):
        _ = overseer.get_mytardis_set_up()
        assert error_str in caplog.text  # nosec


@mock.patch("src.helpers.mt_rest.MyTardisRESTFactory.mytardis_api_request")
def test_get_mytardis_setup_general_error(mock_mytardis_api_request, caplog, overseer):
    mock_mytardis_api_request.side_effect = IOError()
    error_str = "Non-HTTP exception in Overseer.get_mytardis_set_up"
    with pytest.raises(IOError):
        _ = overseer.get_mytardis_set_up()
        assert error_str in caplog.text  # nosec


@responses.activate
def test_get_mytardis_setup_no_objects(
    caplog,
    config_dict,
    overseer,
    response_dict_not_found,
):

    responses.add(
        responses.GET,
        f"{config_dict['hostname']}/api/v1/introspection",
        json=(response_dict_not_found),
        status=200,
    )
    caplog.set_level(logging.ERROR)
    error_str = (
        "MyTardis introspection did not return any data when called from "
        "Overseer.get_mytardis_set_up"
    )
    with pytest.raises(ValueError, match=error_str):
        _ = overseer.get_mytardis_set_up()
        assert error_str in caplog.text  # nosec


@responses.activate
def test_get_mytardis_setup_too_many_objects(
    caplog,
    config_dict,
    overseer,
    introspection_response_dict,
):
    test_dict = introspection_response_dict
    test_dict["objects"].append("Some Fake Data")
    responses.add(
        responses.GET,
        f"{config_dict['hostname']}/api/v1/introspection",
        json=(test_dict),
        status=200,
    )
    caplog.set_level(logging.ERROR)
    log_error_str = (
        "MyTardis introspection returned more than one object when called from "
        "Overseer.get_mytardis_set_up\n"
        f"Returned response was: {test_dict}"
    )
    error_str = (
        "MyTardis introspection returned more than one object when called from "
        "Overseer.get_mytardis_set_up"
    )
    with pytest.raises(ValueError, match=error_str):
        _ = overseer.get_mytardis_set_up()
        assert log_error_str in caplog.text  # nosec


@responses.activate
def test_get_objects(
    config_dict,
    overseer,
    project_response_dict,
):
    object_type = "project"
    search_target = "pids"
    search_string = "Project_1"
    responses.add(
        responses.GET,
        f"{config_dict['hostname']}/api/v1/{object_type}",
        json=(project_response_dict),
        match=[
            matchers.query_param_matcher(
                {search_target: search_string},
            ),
        ],
        status=200,
    )

    assert (  # nosec
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
    config_dict,
    overseer,
):
    object_type = "project"
    search_target = "pids"
    search_string = "Project_1"
    responses.add(
        responses.GET,
        f"{config_dict['hostname']}/api/v1/{object_type}",
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
    assert (  # nosec
        overseer.get_objects(
            object_type,
            search_target,
            search_string,
        )
        is None
    )
    assert warning_str in caplog.text  # nosec


@mock.patch("src.helpers.mt_rest.MyTardisRESTFactory.mytardis_api_request")
def test_get_objects_general_error(
    mock_mytardis_api_request,
    caplog,
    overseer,
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
        assert error_str in caplog.text  # nosec


@responses.activate
def test_get_objects_no_objects(
    config_dict,
    overseer,
    response_dict_not_found,
):
    object_type = "project"
    search_target = "pids"
    search_string = "Project_1"
    responses.add(
        responses.GET,
        f"{config_dict['hostname']}/api/v1/{object_type}",
        json=(response_dict_not_found),
        match=[
            matchers.query_param_matcher(
                {search_target: search_string},
            ),
        ],
        status=200,
    )

    assert (  # nosec
        overseer.get_objects(
            object_type,
            search_target,
            search_string,
        )
        == []
    )


@responses.activate
def test_get_uris(
    config_dict,
    overseer,
    project_response_dict,
):
    object_type = "project"
    search_target = "pids"
    search_string = "Project_1"
    responses.add(
        responses.GET,
        f"{config_dict['hostname']}/api/v1/{object_type}",
        json=(project_response_dict),
        match=[
            matchers.query_param_matcher(
                {search_target: search_string},
            ),
        ],
        status=200,
    )
    assert (
        overseer.get_uris(  # nosec
            object_type,
            search_target,
            search_string,
        )
        == ["/api/v1/project/1/"]
    )


@responses.activate
def test_get_uris_no_objects(
    config_dict,
    overseer,
    response_dict_not_found,
):
    object_type = "project"
    search_target = "pids"
    search_string = "Project_1"
    responses.add(
        responses.GET,
        f"{config_dict['hostname']}/api/v1/{object_type}",
        json=(response_dict_not_found),
        match=[
            matchers.query_param_matcher(
                {search_target: search_string},
            ),
        ],
        status=200,
    )
    assert (  # nosec
        overseer.get_uris(
            object_type,
            search_target,
            search_string,
        )
        == []
    )


@responses.activate
def test_get_uris_malformed_return_dict(
    caplog,
    config_dict,
    overseer,
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
        f"{config_dict['hostname']}/api/v1/{object_type}",
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
        assert error_str in caplog.text  # nosec


@responses.activate
def test_get_uris_ensure_http_errors_caught_by_get_objects(
    caplog,
    config_dict,
    overseer,
):
    caplog.set_level(logging.WARNING)
    object_type = "project"
    search_target = "pids"
    search_string = "Project_1"
    responses.add(
        responses.GET,
        f"{config_dict['hostname']}/api/v1/{object_type}",
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
    assert (  # nosec
        overseer.get_uris(
            object_type,
            search_target,
            search_string,
        )
        is None
    )
    assert warning_str in caplog.text  # nosec


@mock.patch("src.helpers.mt_rest.MyTardisRESTFactory.mytardis_api_request")
def test_get_uris_general_error(
    mock_mytardis_api_request,
    overseer,
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


@responses.activate
def test_get_uris_by_identifier(
    config_dict,
    overseer,
    project_response_dict,
):
    object_type = "project"
    search_target = "pids"
    search_string = "Project_1"
    responses.add(
        responses.GET,
        f"{config_dict['hostname']}/api/v1/{object_type}",
        json=(project_response_dict),
        match=[
            matchers.query_param_matcher(
                {search_target: search_string},
            ),
        ],
        status=200,
    )
    assert (
        overseer.get_uris_by_identifier(  # nosec
            object_type,
            search_string,
        )
        == ["/api/v1/project/1/"]
    )


def test_get_uris_by_identifier_app_not_used(
    caplog,
    overseer,
):
    caplog.set_level(logging.WARNING)
    object_type = "project"
    search_string = "Project_1"
    overseer.mytardis_setup["objects_with_ids"] = []
    warning_str = (
        "The identifiers app is not installed in the instance of MyTardis, "
        "or there are no objects defined in OBJECTS_WITH_IDENTIFIERS in "
        "settings.py"
    )
    assert overseer.get_uris_by_identifier(object_type, search_string) is None  # nosec
    assert warning_str in caplog.text  # nosec


def test_get_uris_by_identifier_object_not_set_up_for_ids(
    caplog,
    overseer,
):
    caplog.set_level(logging.WARNING)
    object_type = "project"
    search_string = "Project_1"
    overseer.mytardis_setup["objects_with_ids"] = ["experiment"]
    warning_str = (
        f"The object type, {object_type}, is not present in "
        "OBJECTS_WITH_IDENTIFIERS defined in settings.py"
    )
    assert overseer.get_uris_by_identifier(object_type, search_string) is None  # nosec
    assert warning_str in caplog.text  # nosec


@responses.activate
def test_get_project_uri(
    overseer,
    config_dict,
    project_response_dict,
    search_with_no_uri_dict,
):
    object_type = "project"
    responses.add(
        responses.GET,
        f"{config_dict['hostname']}/api/v1/project",
        match=[
            matchers.query_param_matcher({"pids": search_with_no_uri_dict["project"]})
        ],
        status=200,
        json=(project_response_dict),
    )
    assert overseer.get_object_uri(  # nosec
        object_type, search_with_no_uri_dict["project"]
    ) == [  #
        "/api/v1/project/1/"
    ]


@responses.activate
def test_get_project_uri_fall_back_search(
    overseer,
    config_dict,
    project_response_dict,
    search_with_no_uri_dict,
    response_dict_not_found,
):
    object_type = "project"
    responses.add(
        responses.GET,
        f"{config_dict['hostname']}/api/v1/project",
        match=[
            matchers.query_param_matcher({"pids": search_with_no_uri_dict["project"]})
        ],
        status=200,
        json=(response_dict_not_found),
    )
    responses.add(
        responses.GET,
        f"{config_dict['hostname']}/api/v1/project",
        match=[
            matchers.query_param_matcher({"name": search_with_no_uri_dict["project"]})
        ],
        status=200,
        json=(project_response_dict),
    )
    assert overseer.get_object_uri(  # nosec
        object_type, search_with_no_uri_dict["project"]
    ) == ["/api/v1/project/1/"]


@responses.activate
def test_get_project_uri_not_found(
    overseer,
    config_dict,
    search_with_no_uri_dict,
    response_dict_not_found,
):
    object_type = "project"
    responses.add(
        responses.GET,
        f"{config_dict['hostname']}/api/v1/project",
        match=[
            matchers.query_param_matcher({"pids": search_with_no_uri_dict["project"]})
        ],
        status=200,
        json=(response_dict_not_found),
    )
    responses.add(
        responses.GET,
        f"{config_dict['hostname']}/api/v1/project",
        match=[
            matchers.query_param_matcher({"name": search_with_no_uri_dict["project"]})
        ],
        status=200,
        json=(response_dict_not_found),
    )
    assert (  # nosec
        overseer.get_object_uri(object_type, search_with_no_uri_dict["project"]) == []
    )


@responses.activate
def test_get_experiment_uri(
    overseer,
    config_dict,
    experiment_response_dict,
    search_with_no_uri_dict,
):
    object_type = "experiment"
    responses.add(
        responses.GET,
        f"{config_dict['hostname']}/api/v1/experiment",
        match=[
            matchers.query_param_matcher(
                {"pids": search_with_no_uri_dict["experiment"]}
            )
        ],
        status=200,
        json=(experiment_response_dict),
    )
    assert overseer.get_object_uri(  # nosec
        object_type, search_with_no_uri_dict["experiment"]
    ) == ["/api/v1/experiment/1/"]


@responses.activate
def test_get_experiment_uri_fall_back_search(
    overseer,
    config_dict,
    experiment_response_dict,
    search_with_no_uri_dict,
    response_dict_not_found,
):
    object_type = "experiment"
    responses.add(
        responses.GET,
        f"{config_dict['hostname']}/api/v1/experiment",
        match=[
            matchers.query_param_matcher(
                {"pids": search_with_no_uri_dict["experiment"]}
            )
        ],
        status=200,
        json=(response_dict_not_found),
    )
    responses.add(
        responses.GET,
        f"{config_dict['hostname']}/api/v1/experiment",
        match=[
            matchers.query_param_matcher(
                {"title": search_with_no_uri_dict["experiment"]}
            )
        ],
        status=200,
        json=(experiment_response_dict),
    )
    assert overseer.get_object_uri(  # nosec
        object_type, search_with_no_uri_dict["experiment"]
    ) == ["/api/v1/experiment/1/"]


@responses.activate
def test_get_experiment_uri_not_found(
    overseer,
    config_dict,
    search_with_no_uri_dict,
    response_dict_not_found,
):
    object_type = "experiment"
    responses.add(
        responses.GET,
        f"{config_dict['hostname']}/api/v1/experiment",
        match=[
            matchers.query_param_matcher(
                {"pids": search_with_no_uri_dict["experiment"]}
            )
        ],
        status=200,
        json=(response_dict_not_found),
    )
    responses.add(
        responses.GET,
        f"{config_dict['hostname']}/api/v1/experiment",
        match=[
            matchers.query_param_matcher(
                {"title": search_with_no_uri_dict["experiment"]}
            )
        ],
        status=200,
        json=(response_dict_not_found),
    )
    assert (  # nosec
        overseer.get_object_uri(object_type, search_with_no_uri_dict["experiment"])
        == []
    )


@responses.activate
def test_get_dataset_uri(
    overseer,
    config_dict,
    dataset_response_dict,
    search_with_no_uri_dict,
):
    object_type = "dataset"
    responses.add(
        responses.GET,
        f"{config_dict['hostname']}/api/v1/dataset",
        match=[
            matchers.query_param_matcher({"pids": search_with_no_uri_dict["dataset"]})
        ],
        status=200,
        json=(dataset_response_dict),
    )
    assert overseer.get_object_uri(  # nosec
        object_type, search_with_no_uri_dict["dataset"]
    ) == ["/api/v1/dataset/1/"]


@responses.activate
def test_get_dataset_uri_fall_back_search(
    overseer,
    config_dict,
    dataset_response_dict,
    search_with_no_uri_dict,
    response_dict_not_found,
):
    object_type = "dataset"
    responses.add(
        responses.GET,
        f"{config_dict['hostname']}/api/v1/dataset",
        match=[
            matchers.query_param_matcher({"pids": search_with_no_uri_dict["dataset"]})
        ],
        status=200,
        json=(response_dict_not_found),
    )
    responses.add(
        responses.GET,
        f"{config_dict['hostname']}/api/v1/dataset",
        match=[
            matchers.query_param_matcher(
                {"description": search_with_no_uri_dict["dataset"]}
            )
        ],
        status=200,
        json=(dataset_response_dict),
    )
    assert overseer.get_object_uri(  # nosec
        object_type, search_with_no_uri_dict["dataset"]
    ) == ["/api/v1/dataset/1/"]


@responses.activate
def test_get_dataset_uri_not_found(
    overseer,
    config_dict,
    search_with_no_uri_dict,
    response_dict_not_found,
):
    object_type = "dataset"
    responses.add(
        responses.GET,
        f"{config_dict['hostname']}/api/v1/dataset",
        match=[
            matchers.query_param_matcher({"pids": search_with_no_uri_dict["dataset"]})
        ],
        status=200,
        json=(response_dict_not_found),
    )
    responses.add(
        responses.GET,
        f"{config_dict['hostname']}/api/v1/dataset",
        match=[
            matchers.query_param_matcher(
                {"description": search_with_no_uri_dict["dataset"]}
            )
        ],
        status=200,
        json=(response_dict_not_found),
    )
    assert (  # nosec
        overseer.get_object_uri(object_type, search_with_no_uri_dict["dataset"]) == []
    )


@responses.activate
def test_get_instrument_uri(
    overseer,
    config_dict,
    instrument_response_dict,
    search_with_no_uri_dict,
):
    object_type = "instrument"
    responses.add(
        responses.GET,
        f"{config_dict['hostname']}/api/v1/instrument",
        match=[
            matchers.query_param_matcher(
                {"pids": search_with_no_uri_dict["instrument"]}
            )
        ],
        status=200,
        json=(instrument_response_dict),
    )
    assert overseer.get_object_uri(  # nosec
        object_type, search_with_no_uri_dict["instrument"]
    ) == ["/api/v1/instrument/1/"]


@responses.activate
def test_get_instrument_uri_fall_back_search(
    overseer,
    config_dict,
    instrument_response_dict,
    search_with_no_uri_dict,
    response_dict_not_found,
):
    object_type = "instrument"
    responses.add(
        responses.GET,
        f"{config_dict['hostname']}/api/v1/instrument",
        match=[
            matchers.query_param_matcher(
                {"pids": search_with_no_uri_dict["instrument"]}
            )
        ],
        status=200,
        json=(response_dict_not_found),
    )
    responses.add(
        responses.GET,
        f"{config_dict['hostname']}/api/v1/instrument",
        match=[
            matchers.query_param_matcher(
                {"name": search_with_no_uri_dict["instrument"]}
            )
        ],
        status=200,
        json=(instrument_response_dict),
    )
    assert overseer.get_object_uri(  # nosec
        object_type, search_with_no_uri_dict["instrument"]
    ) == ["/api/v1/instrument/1/"]


@responses.activate
def test_get_instrument_uri_not_found(
    overseer,
    config_dict,
    search_with_no_uri_dict,
    response_dict_not_found,
):
    object_type = "instrument"
    responses.add(
        responses.GET,
        f"{config_dict['hostname']}/api/v1/instrument",
        match=[
            matchers.query_param_matcher(
                {"pids": search_with_no_uri_dict["instrument"]}
            )
        ],
        status=200,
        json=(response_dict_not_found),
    )
    responses.add(
        responses.GET,
        f"{config_dict['hostname']}/api/v1/instrument",
        match=[
            matchers.query_param_matcher(
                {"name": search_with_no_uri_dict["instrument"]}
            )
        ],
        status=200,
        json=(response_dict_not_found),
    )
    assert (  # nosec
        overseer.get_object_uri(object_type, search_with_no_uri_dict["instrument"])
        == []
    )
