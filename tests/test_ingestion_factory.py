# NB: For the purposes of testing the IngestionFactory codebase a YAML specific
# implementation of the IngestionFactory is used. This concrete class defines
# the YAMLSmelter class to be the smelter implementation. As this class is fully
# tested, there is little risk in using this as a test case.

import logging
from pathlib import Path

import mock
import pytest
import responses
from pytest import fixture
from responses import matchers

from src.ingestion_factory import IngestionFactory
from src.smelters import Smelter

from .conftest import (
    config_dict,
    dataset_response_dict,
    experiment_response_dict,
    institution_response_dict,
    processed_introspection_response,
    project_response_dict,
    response_dict_not_found,
    smelter,
)

logger = logging.getLogger(__name__)
logger.propagate = True

GLOB_STRING = "*.test"


@fixture
def factory(
    smelter,
    config_dict,
    processed_introspection_response,
):
    with mock.patch(
        "src.ingestion_factory.factory.IngestionFactory.get_smelter"
    ) as mock_get_smelter:
        mock_get_smelter.return_value = smelter
        with mock.patch.object(
            smelter, "get_file_type_for_input_files"
        ) as mock_get_file_type_for_input_files:
            mock_get_file_type_for_input_files.return_value = GLOB_STRING
            with mock.patch(
                "src.overseers.overseer.Overseer.get_mytardis_set_up"
            ) as mock_get_mytardis_setup:
                mock_get_mytardis_setup.return_value = processed_introspection_response
                IngestionFactory.__abstractmethods__ = set()
                return IngestionFactory(config_dict)


@fixture
def search_with_no_uri_dict():
    return {"institution": "Uni RoR"}


def mock_smelter_get_objects_in_input_file(file_path):
    file_path = Path(file_path)
    with open(file_path) as test_file:
        for line in test_file.readlines():
            if line.startswith("project"):
                return ["project"]
            if line.startswith("experiment"):
                return ["experiment"]
            if line.startswith("dataset"):
                return ["dataset"]
            if line.startswith("datafile"):
                return ["datafile"]
    return [None]


def test_build_object_lists(datadir, factory):
    with mock.patch.object(
        factory.smelter, "get_objects_in_input_file"
    ) as mock_get_inputs_in_input_file:
        mock_get_inputs_in_input_file.side_effect = (
            mock_smelter_get_objects_in_input_file
        )
        assert factory.build_object_lists(
            Path(datadir / "projects.test"), "project"
        ) == [Path(datadir / "projects.test")]
        assert (
            factory.build_object_lists(Path(datadir / "projects.test"), "experiment")
            == []
        )
        assert set(factory.build_object_lists(datadir, "project")) == set(
            [
                Path(datadir / "projects.test"),
                Path(datadir / "projects_2.test"),
                Path(datadir / "projects_3.test"),
            ]
        )
        assert factory.build_object_lists(datadir, "experiment") == [
            Path(datadir / "experiment.test")
        ]
        assert factory.build_object_lists(datadir, "dataset") == [
            Path(datadir / "dataset.test")
        ]
        assert factory.build_object_lists(datadir, "datafile") == [
            Path(datadir / "datafile.test")
        ]


@responses.activate
def test_replace_search_term_with_uri(
    factory,
    institution_response_dict,
    config_dict,
    search_with_no_uri_dict,
):
    responses.add(
        responses.GET,
        f"{config_dict['hostname']}/api/v1/institution",
        match=[
            matchers.query_param_matcher(
                {"pids": search_with_no_uri_dict["institution"]}
            )
        ],
        status=200,
        json=(institution_response_dict),
    )
    object_type = "institution"
    assert factory.replace_search_term_with_uri(
        object_type, search_with_no_uri_dict, "name"
    ) == {"institution": ["/api/v1/institution/1/"]}


@responses.activate
def test_replace_search_term_with_uri_fallback_search(
    factory,
    response_dict_not_found,
    institution_response_dict,
    config_dict,
    search_with_no_uri_dict,
):
    responses.add(
        responses.GET,
        f"{config_dict['hostname']}/api/v1/institution",
        match=[
            matchers.query_param_matcher(
                {"pids": search_with_no_uri_dict["institution"]}
            )
        ],
        status=200,
        json=(response_dict_not_found),
    )
    responses.add(
        responses.GET,
        f"{config_dict['hostname']}/api/v1/institution",
        match=[
            matchers.query_param_matcher(
                {"name": search_with_no_uri_dict["institution"]}
            )
        ],
        status=200,
        json=(institution_response_dict),
    )
    object_type = "institution"
    assert factory.replace_search_term_with_uri(
        object_type, search_with_no_uri_dict, "name"
    ) == {"institution": ["/api/v1/institution/1/"]}


@responses.activate
def test_replace_search_term_with_uri_not_found(
    factory,
    response_dict_not_found,
    config_dict,
    search_with_no_uri_dict,
):
    responses.add(
        responses.GET,
        f"{config_dict['hostname']}/api/v1/institution",
        match=[
            matchers.query_param_matcher(
                {"pids": search_with_no_uri_dict["institution"]}
            )
        ],
        status=200,
        json=(response_dict_not_found),
    )
    responses.add(
        responses.GET,
        f"{config_dict['hostname']}/api/v1/institution",
        match=[
            matchers.query_param_matcher(
                {"name": search_with_no_uri_dict["institution"]}
            )
        ],
        status=200,
        json=(response_dict_not_found),
    )
    object_type = "institution"
    assert (
        factory.replace_search_term_with_uri(
            object_type, search_with_no_uri_dict, "name"
        )
        == search_with_no_uri_dict
    )


@responses.activate
def test_replace_search_term_with_uri_http_error(
    factory,
    institution_response_dict,
    config_dict,
    search_with_no_uri_dict,
):
    responses.add(
        responses.GET,
        f"{config_dict['hostname']}/api/v1/institution",
        match=[
            matchers.query_param_matcher(
                {"pids": search_with_no_uri_dict["institution"]}
            )
        ],
        status=404,
        json=(institution_response_dict),
    )
    responses.add(
        responses.GET,
        f"{config_dict['hostname']}/api/v1/institution",
        match=[
            matchers.query_param_matcher(
                {"name": search_with_no_uri_dict["institution"]}
            )
        ],
        status=404,
        json=(institution_response_dict),
    )
    object_type = "institution"
    assert (
        factory.replace_search_term_with_uri(
            object_type, search_with_no_uri_dict, "name"
        )
        == search_with_no_uri_dict
    )


@responses.activate
def test_get_project_uri(factory, config_dict, project_response_dict):
    pass
