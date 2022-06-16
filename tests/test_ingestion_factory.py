# NB: For the purposes of testing the IngestionFactory codebase a YAML specific
# implementation of the IngestionFactory is used. This concrete class defines
# the YAMLSmelter class to be the smelter implementation. As this class is fully
# tested, there is little risk in using this as a test case.

import logging
from copy import deepcopy
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
    instrument_response_dict,
    processed_introspection_response,
    project_response_dict,
    response_dict_not_found,
    smelter,
)

logger = logging.getLogger(__name__)
logger.propagate = True


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
        with mock.patch(
            "src.overseers.overseer.Overseer.get_mytardis_set_up"
        ) as mock_get_mytardis_setup:
            mock_get_mytardis_setup.return_value = processed_introspection_response
            IngestionFactory.__abstractmethods__ = set()
            return IngestionFactory(config_dict)


@fixture
def search_with_no_uri_dict():
    return {
        "institution": "Uni RoR",
        "project": "Test_Project",
        "experiment": "Test_Experiment",
        "dataset": "Test_Dataset",
        "instrument": "Instrument_1",
    }


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


def test_build_object_dict(
    datadir,
    factory,
    smelter,
):
    factory.smelter = smelter
    with mock.patch.object(
        factory.smelter, "get_input_file_paths"
    ) as mock_get_input_file_paths:
        mock_get_input_file_paths.return_value = [
            Path(datadir / "datafile.test"),
            Path(datadir / "dataset.test"),
            Path(datadir / "experiment.test"),
            Path(datadir / "projects_2.test"),
            Path(datadir / "projects_3.test"),
            Path(datadir / "projects.test"),
        ]
        with mock.patch.object(
            factory.smelter, "get_objects_in_input_file"
        ) as mock_get_inputs_in_input_file:
            mock_get_inputs_in_input_file.side_effect = (
                mock_smelter_get_objects_in_input_file
            )
            object_types = ("project", "experiment", "dataset", "datafile")
            expected_dict = {
                "datafile": [Path(datadir / "datafile.test")],
                "dataset": [Path(datadir / "dataset.test")],
                "experiment": [Path(datadir / "experiment.test")],
                "project": [
                    Path(datadir / "projects_2.test"),
                    Path(datadir / "projects_3.test"),
                    Path(datadir / "projects.test"),
                ],
            }
            assert factory._build_object_dict(datadir, object_types) == expected_dict


@pytest.mark.parametrize(
    "test_values,expected_outcomes", [("Project_1", True), ("Project_2", False)]
)
def test_verify_object_unblocked(
    factory,
    test_values,
    expected_outcomes,
):
    factory.blocked_ids["project"] = ["Project_2"]
    assert factory._verify_object_unblocked("project", test_values) == expected_outcomes


def test_block_object(
    factory,
    tidied_project_dictionary,
):
    expected_output = ["Test Project", "Project_1", "Test_Project", "Project_Test_1"]
    factory._block_object(
        "project",
        tidied_project_dictionary,
    )
    for output in expected_output:
        assert output in factory.blocked_ids["project"]


def test_match_or_block_object(
    factory,
    tidied_project_dictionary,
    project_response_dict,
):
    comparison_keys = factory.project_comparison_keys
    assert (
        factory._match_or_block_object(
            "project",
            tidied_project_dictionary,
            project_response_dict["objects"][0],
            comparison_keys,
        )
        == "/api/v1/project/1/"
    )


def test_match_or_block_object_bad_response_with_no_uri(
    caplog,
    factory,
    tidied_project_dictionary,
    project_response_dict,
):
    caplog.set_level(logging.WARNING)
    comparison_keys = factory.project_comparison_keys
    project_response_dict["objects"][0].pop("resource_uri")
    warning_str = (
        "Unable to find the resource_uri field in the mytardis project "
        "dictionary. This suggests an incomplete or malformed dictionary. The "
    )
    assert (
        factory._match_or_block_object(
            "project",
            tidied_project_dictionary,
            project_response_dict["objects"][0],
            comparison_keys,
        )
        is None
    )
    assert warning_str in caplog.text


def test_match_or_block_object_blocked_because_of_mismatch(
    caplog,
    factory,
    tidied_project_dictionary,
    project_response_dict,
):
    caplog.set_level(logging.WARNING)
    comparison_keys = factory.project_comparison_keys
    warning_str = (
        "Mismatch in dictionaries. Since we are unable to uniquely identify the "
        "project, and given the potential for mis-assigning sensitive data "
        "this project object will not be created. The ID will also be blocked "
    )
    tidied_project_dictionary[
        "description"
    ] = "The test project for the purposes of testing"
    assert (
        factory._match_or_block_object(
            "project",
            tidied_project_dictionary,
            project_response_dict["objects"][0],
            comparison_keys,
        )
        is None
    )
    expected_output = ["Test Project", "Project_1", "Test_Project", "Project_Test_1"]
    for output in expected_output:
        assert output in factory.blocked_ids["project"]
    assert warning_str in caplog.text


def test_remove_blocked_parents_from_object_dict_remove_none(
    factory,
    tidied_experiment_dictionary,
):
    assert (
        factory._remove_blocked_parents_from_object_dict(
            "project",
            "experiment",
            tidied_experiment_dictionary,
        )
        == tidied_experiment_dictionary
    )


def test_remove_blocked_parents_from_object_dict_remove_all(
    caplog,
    factory,
    tidied_experiment_dictionary,
):
    caplog.set_level(logging.WARNING)
    factory.blocked_ids["project"] = ["Project_1", "Test_Project"]
    warning_str = (
        f"Some parents of the experiment "
        f"{tidied_experiment_dictionary['title']} have been removed "
        "due to previously being blocked. The parents removed are "
    )
    assert (
        factory._remove_blocked_parents_from_object_dict(
            "project",
            "experiment",
            tidied_experiment_dictionary,
        )
        is None
    )
    assert warning_str in caplog.text


def test_remove_blocked_parents_from_object_dict_remove_some(
    caplog,
    factory,
    tidied_experiment_dictionary,
):
    caplog.set_level(logging.WARNING)
    factory.blocked_ids["project"] = ["Project_1"]
    test_dictionary = deepcopy(tidied_experiment_dictionary)
    test_dictionary["projects"] = ["Test_Project"]
    warning_str = (
        f"Some parents of the experiment "
        f"{tidied_experiment_dictionary['title']} have been removed "
        "due to previously being blocked. The parents removed are "
        f"{['Project_1']}"
    )
    assert (
        factory._remove_blocked_parents_from_object_dict(
            "project",
            "experiment",
            tidied_experiment_dictionary,
        )
        == test_dictionary
    )
    assert warning_str in caplog.text


def test_match_or_block_project(
    factory,
    tidied_project_dictionary,
    project_response_dict,
):
    assert (
        factory._match_or_block_project(
            tidied_project_dictionary,
            project_response_dict["objects"][0],
        )
        == "/api/v1/project/1/"
    )


def test_match_or_block_project_no_projects_enabled(
    caplog,
    factory,
    tidied_project_dictionary,
    project_response_dict,
):
    caplog.set_level(logging.WARNING)
    warning_str = (
        "Projects have not been enabled for this instance of MyTardis. Please contact "
        "your MyTardis sysadmin for further information."
    )
    factory.mytardis_setup["projects_enabled"] = False
    assert (
        factory._match_or_block_project(
            tidied_project_dictionary,
            project_response_dict["objects"][0],
        )
        is None
    )
    assert warning_str in caplog.text


def test_match_or_block_project_no_persistent_id_when_ids_enabled(
    caplog,
    factory,
    tidied_project_dictionary,
    project_response_dict,
):
    caplog.set_level(logging.WARNING)
    tidied_project_dictionary.pop("persistent_id")
    warning_str = (
        f"No persistent ID found for project {tidied_project_dictionary['name']}"
    )
    assert (
        factory._match_or_block_project(
            tidied_project_dictionary,
            project_response_dict["objects"][0],
        )
        == "/api/v1/project/1/"
    )
    assert warning_str in caplog.text


def test_match_or_block_project_mismatch(
    factory,
    tidied_project_dictionary,
    project_response_dict,
):
    tidied_project_dictionary[
        "description"
    ] = "The test project for the purposes of testing"
    assert (
        factory._match_or_block_project(
            tidied_project_dictionary,
            project_response_dict["objects"][0],
        )
        is None
    )
    expected_output = ["Test Project", "Project_1", "Test_Project", "Project_Test_1"]
    for output in expected_output:
        assert output in factory.blocked_ids["project"]


def test_match_or_block_project_previously_blocked(
    caplog,
    factory,
    tidied_project_dictionary,
    project_response_dict,
):
    caplog.set_level(logging.WARNING)
    factory.blocked_ids["project"] = [
        "Test Project",
        "Project_1",
        "Test_Project",
        "Project_Test_1",
    ]
    warning_str = (
        f"The project_id, {tidied_project_dictionary['persistent_id']} has been previously "
        "blocked for ingestion due to a mismatch. Please refer to the log files to identify "
        "the cause of this issue."
    )
    assert (
        factory._match_or_block_project(
            tidied_project_dictionary,
            project_response_dict["objects"][0],
        )
        is None
    )
    assert warning_str in caplog.text


def test_match_or_block_experiment(
    factory,
    tidied_experiment_dictionary,
    experiment_response_dict,
):
    assert (
        factory._match_or_block_experiment(
            tidied_experiment_dictionary,
            experiment_response_dict["objects"][0],
        )
        == "/api/v1/experiment/1/"
    )


def test_match_or_block_project_no_projects_enabled(
    caplog,
    factory,
    tidied_project_dictionary,
    project_response_dict,
):
    caplog.set_level(logging.WARNING)
    warning_str = (
        "Projects have not been enabled for this instance of MyTardis. Please contact "
        "your MyTardis sysadmin for further information."
    )
    factory.mytardis_setup["projects_enabled"] = False
    assert (
        factory._match_or_block_project(
            tidied_project_dictionary,
            project_response_dict["objects"][0],
        )
        is None
    )
    assert warning_str in caplog.text


def test_match_or_block_project_no_persistent_id_when_ids_enabled(
    caplog,
    factory,
    tidied_project_dictionary,
    project_response_dict,
):
    caplog.set_level(logging.WARNING)
    tidied_project_dictionary.pop("persistent_id")
    warning_str = (
        f"No persistent ID found for project {tidied_project_dictionary['name']}"
    )
    assert (
        factory._match_or_block_project(
            tidied_project_dictionary,
            project_response_dict["objects"][0],
        )
        == "/api/v1/project/1/"
    )
    assert warning_str in caplog.text


def test_match_or_block_project_mismatch(
    factory,
    tidied_project_dictionary,
    project_response_dict,
):
    tidied_project_dictionary[
        "description"
    ] = "The test project for the purposes of testing"
    assert (
        factory._match_or_block_project(
            tidied_project_dictionary,
            project_response_dict["objects"][0],
        )
        is None
    )
    expected_output = ["Test Project", "Project_1", "Test_Project", "Project_Test_1"]
    for output in expected_output:
        assert output in factory.blocked_ids["project"]


def test_match_or_block_project_previously_blocked(
    caplog,
    factory,
    tidied_project_dictionary,
    project_response_dict,
):
    caplog.set_level(logging.WARNING)
    factory.blocked_ids["project"] = [
        "Test Project",
        "Project_1",
        "Test_Project",
        "Project_Test_1",
    ]
    warning_str = (
        f"The project_id, {tidied_project_dictionary['persistent_id']} has been previously "
        "blocked for ingestion due to a mismatch. Please refer to the log files to identify "
        "the cause of this issue."
    )
    assert (
        factory._match_or_block_project(
            tidied_project_dictionary,
            project_response_dict["objects"][0],
        )
        is None
    )
    assert warning_str in caplog.text
