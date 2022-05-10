# pylint: disable=missing-function-docstring

"""Tests of the Overseer class and its functions"""
import logging

import mock
import pytest
import responses
from pytest import fixture
from requests.exceptions import HTTPError
from responses import matchers

from src.overseers import Overseer

from .conftest import (
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


def test_staticmethod_resource_uri_to_id():
    test_uri = "/api/v1/user/10/"
    assert Overseer.resource_uri_to_id(test_uri) == 10


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

    assert overseer.get_mytardis_set_up() == mytardis_setup_dict


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
        assert error_str in caplog.text


@mock.patch("src.helpers.mt_rest.MyTardisRESTFactory.mytardis_api_request")
def test_get_mytardis_setup_general_error(mock_mytardis_api_request, caplog, overseer):
    mock_mytardis_api_request.side_effect = IOError()
    error_str = "Non-HTTP exception in Overseer.get_mytardis_set_up"
    with pytest.raises(IOError):
        _ = overseer.get_mytardis_set_up()
        assert error_str in caplog.text


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
        assert error_str in caplog.text


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
        assert log_error_str in caplog.text


@responses.activate
def test_get_object(
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
        overseer.get_objects(
            object_type,
            search_target,
            search_string,
        )
        == project_response_dict["objects"]
    )
