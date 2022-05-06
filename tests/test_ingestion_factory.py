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

from src.factory import IngestionFactory
from src.smelter import Smelter

from .conftest import config_dict, processed_introspection_response, smelter

logger = logging.getLogger(__name__)
logger.propagate = True


@mock.patch("src.overseers.overseer.Overseer.get_mytardis_set_up")
@mock.patch("src.smelter.smelter.Smelter.get_file_type_for_input_files")
@mock.patch("src.factory.factory.IngestionFactory.get_smelter")
@fixture
def factory(
    mock_get_smelter,
    mock_get_file_type_for_input_files,
    mock_get_mytardis_setup,
    smelter,
):
    mock_get_smelter.return_value = smelter
    mock_get_file_type_for_input_files.return_value = "*.test"
    mock_get_mytardis_setup.return_value = processed_introspection_response
    IngestionFactory.__abstractmethods__ = set()


search_with_no_uri_dict = {"institution": "Uni RoR"}


@pytest.mark.xfail
@responses.activate
def test_build_object_lists(datadir):
    responses.add(
        responses.GET,
        f"{config_dict['hostname']}/api/v1/introspection",
        status=200,
        json=(introspection_response_dict),
    )
    factory = YAMLIngestionFactory(config_dict)
    test_projects = [
        Path(datadir / "test_mixed.yaml"),
        Path(datadir / "test_project.yaml"),
    ]
    test_experiments = [
        Path(datadir / "test_experiment.yaml"),
        Path(datadir / "test_mixed.yaml"),
    ]
    test_datasets = [
        Path(datadir / "test_dataset.yaml"),
    ]
    test_datafiles = [
        Path(datadir / "test_datafile.yaml"),
    ]
    assert factory.build_object_lists(datadir, "project") == test_projects
    assert factory.build_object_lists(datadir, "experiment") == test_experiments
    assert factory.build_object_lists(datadir, "dataset") == test_datasets
    assert factory.build_object_lists(datadir, "datafile") == test_datafiles


@responses.activate
def test_build_object_lists_rename_this(datadir):
    object_type = "institution"
    pid = "Uni RoR"
    responses.add(
        responses.GET,
        f"{config_dict['hostname']}/api/v1/introspection",
        status=200,
        json=(introspection_response_dict),
    )
    responses.add(
        responses.GET,
        f"{config_dict['hostname']}/api/v1/institution",
        match=[matchers.query_param_matcher({"pids": pid})],
        status=200,
        json=(institution_response_dict),
    )
    factory = YAMLIngestionFactory(config_dict)
    assert factory.replace_search_term_with_uri(
        object_type, search_with_no_uri_dict, "name"
    ) == {"institution": ["/api/v1/institution/1/"]}
