# pylint: disable=missing-module-docstring,missing-function-docstring
# nosec assert_used
# flake8: noqa S101

import copy
import logging
from datetime import datetime
from typing import Any, Dict
from unittest.mock import MagicMock
from urllib.parse import urljoin

import pytest
import responses
from responses import matchers

from src.blueprints.datafile import Datafile, RefinedDatafile
from src.blueprints.dataset import Dataset, RefinedDataset
from src.blueprints.experiment import Experiment, RefinedExperiment
from src.blueprints.project import Project, RefinedProject
from src.config.config import ConnectionConfig
from src.crucible.crucible import Crucible
from src.mytardis_client.endpoints import URI
from src.mytardis_client.objects import MyTardisObject
from src.mytardis_client.response_data import Institution
from src.overseers.overseer import Overseer
from tests.fixtures.fixtures_dataclasses import TestModelFactory


def test_prepare_project(
    overseer: Overseer,
    make_refined_project: TestModelFactory[RefinedProject],
    make_project: TestModelFactory[Project],
    make_institution: TestModelFactory[Institution],
) -> None:

    institution = make_institution(
        name="Test Institution", identifiers=["test-institution-1"]
    )

    refined_project = make_refined_project(
        institution=institution.identifiers,
        start_time=datetime(2000, 1, 1, 12, 0, 0),
    )
    expected_project = make_project(start_time="2000-01-01T12:00:00")

    overseer.get_uris_by_identifier = MagicMock()  # type: ignore[method-assign]
    overseer.get_uris_by_identifier.return_value = [institution.resource_uri]

    crucible = Crucible(overseer)
    prepared_project = crucible.prepare_project(refined_project)

    overseer.get_uris_by_identifier.assert_called_once_with(
        MyTardisObject.INSTITUTION, refined_project.institution[0]
    )

    assert prepared_project == expected_project


@responses.activate
def test_prepare_project_no_matching_institution(
    caplog: pytest.LogCaptureFixture,
    connection: ConnectionConfig,
    crucible: Crucible,
    refined_project: RefinedProject,
    response_dict_not_found: Dict[str, Any],
) -> None:
    caplog.set_level(logging.WARNING)
    responses.get(
        urljoin(connection.api_template, "institution"),
        status=200,
        json=(response_dict_not_found),
    )
    warning = "Unable to identify any institutions that were listed for this project."

    assert crucible.prepare_project(refined_project) is None
    assert warning in caplog.text


@responses.activate
def test_prepare_experiment(
    overseer: Overseer,
    refined_experiment: RefinedExperiment,
    experiment: Experiment,
) -> None:

    refined_experiment.projects = ["test-project-1", "test-project-2"]

    project_uris = [URI("/api/v1/project/1/"), URI("/api/v1/project/2/")]

    expected_experiment = experiment
    expected_experiment.projects = project_uris

    overseer.get_uris_by_identifier = MagicMock()  # type: ignore[method-assign]
    overseer.get_uris_by_identifier.side_effect = [[uri] for uri in project_uris]

    crucible = Crucible(overseer)
    prepared_experiment = crucible.prepare_experiment(refined_experiment)

    assert overseer.get_uris_by_identifier.call_args_list == [
        ((MyTardisObject.PROJECT, refined_experiment.projects[0]),),
        ((MyTardisObject.PROJECT, refined_experiment.projects[1]),),
    ]

    assert prepared_experiment == expected_experiment


@responses.activate
def test_prepare_experiment_no_matching_projects(
    caplog: pytest.LogCaptureFixture,
    connection: ConnectionConfig,
    crucible: Crucible,
    refined_experiment: RefinedExperiment,
    response_dict_not_found: Dict[str, Any],
) -> None:
    caplog.set_level(logging.WARNING)
    responses.get(
        urljoin(connection.api_template, "project"),
        status=200,
        json=(response_dict_not_found),
    )
    warning = (
        "No projects identified for this experiment and projects enabled in MyTardis."
    )

    assert crucible.prepare_experiment(refined_experiment) is None
    assert warning in caplog.text


@responses.activate
def test_prepare_dataset(
    overseer: Overseer,
    refined_dataset: RefinedDataset,
    dataset: Dataset,
) -> None:

    dataset.experiments = [URI("/api/v1/experiment/1/"), URI("/api/v1/experiment/2/")]
    dataset.instrument = URI("/api/v1/instrument/1/")

    overseer.get_uris_by_identifier = MagicMock()  # type: ignore[method-assign]

    def return_uris(object_type: MyTardisObject, identifier: str) -> list[URI]:
        match (object_type, identifier):
            case (MyTardisObject.EXPERIMENT, "Test_Experiment_1"):
                return [dataset.experiments[0]]
            case (MyTardisObject.EXPERIMENT, "Test_Experiment_2"):
                return [dataset.experiments[1]]
            case (MyTardisObject.INSTRUMENT, "Test_Instrument"):
                return [dataset.instrument]
            case _:
                assert False, f"Unexpected args: {(object_type, identifier)}"

    overseer.get_uris_by_identifier.side_effect = return_uris

    crucible = Crucible(overseer)

    prepared_dataset = crucible.prepare_dataset(refined_dataset)
    assert prepared_dataset == dataset


@responses.activate
def test_prepare_dataset_no_matching_experiments(
    caplog: pytest.LogCaptureFixture,
    connection: ConnectionConfig,
    crucible: Crucible,
    refined_dataset: RefinedDataset,
    response_dict_not_found: Dict[str, Any],
) -> None:
    caplog.set_level(logging.WARNING)
    responses.get(
        urljoin(connection.api_template, "experiment"),
        status=200,
        json=(response_dict_not_found),
    )
    warning = "Unable to find experiments associated with this dataset."

    assert crucible.prepare_dataset(refined_dataset) is None
    assert warning in caplog.text


@responses.activate
def test_prepare_dataset_no_matching_instrument(
    caplog: pytest.LogCaptureFixture,
    connection: ConnectionConfig,
    crucible: Crucible,
    refined_dataset: RefinedDataset,
    experiment_response_dict: Dict[str, Any],
    response_dict_not_found: Dict[str, Any],
) -> None:
    caplog.set_level(logging.WARNING)
    responses.get(
        urljoin(connection.api_template, "experiment"),
        match=[
            matchers.query_param_matcher(
                {"identifiers": refined_dataset.experiments[0]}
            )
        ],
        status=200,
        json=experiment_response_dict,
    )
    responses.get(
        urljoin(connection.api_template, "experiment"),
        status=200,
        json=(response_dict_not_found),
    )
    responses.get(
        urljoin(connection.api_template, "instrument"),
        status=200,
        json=(response_dict_not_found),
    )

    assert crucible.prepare_dataset(refined_dataset) is None

    assert any(level == logging.WARNING for _, level, _ in caplog.record_tuples)


@pytest.fixture(name="duplicate_instrument_response_dict")
def fixture_duplicate_instrument_response_dict(
    instrument_response_dict: Dict[str, Any]
) -> Dict[str, Any]:
    instrument = copy.deepcopy(instrument_response_dict["objects"][0])
    instrument["resource_uri"] = "/api/v1/instrument/2/"
    response_dict = copy.deepcopy(instrument_response_dict)
    response_dict["objects"].append(instrument)

    return response_dict


@responses.activate
def test_prepare_dataset_too_many_instruments(
    caplog: pytest.LogCaptureFixture,
    overseer: Overseer,
    refined_dataset: RefinedDataset,
) -> None:
    caplog.set_level(logging.WARNING)

    arg_matcher = {
        MyTardisObject.EXPERIMENT: {
            "Test_Experiment_1": [URI("/api/v1/experiment/1/")],
            "Test_Experiment_2": [URI("/api/v1/experiment/2/")],
        },
        MyTardisObject.INSTRUMENT: {
            "Test_Instrument": [
                URI("/api/v1/instrument/1/"),
                URI("/api/v1/instrument/2/"),
            ],
        },
    }

    def return_uris(object_type: MyTardisObject, identifier: str) -> list[URI]:
        return arg_matcher[object_type][identifier]

    overseer.get_uris_by_identifier = MagicMock()  # type: ignore[method-assign]
    overseer.get_uris_by_identifier.side_effect = return_uris

    crucible = Crucible(overseer)

    warning = (
        "Unable to uniquely identify the instrument associated with the "
        "name or identifier provided. Possible candidates are: "
    )

    assert crucible.prepare_dataset(refined_dataset) is None

    assert any(message.startswith(warning) for message in caplog.messages)


@responses.activate
def test_prepare_datafile(
    overseer: Overseer,
    refined_datafile: RefinedDatafile,
    datafile: Datafile,
) -> None:

    # Assume they are all identifiers for the same dataset, so return same URI repeatedly
    dataset_uris = [[URI("/api/v1/dataset/1/")]] * len(refined_datafile.dataset)
    overseer.get_uris_by_identifier = MagicMock()  # type: ignore[method-assign]
    overseer.get_uris_by_identifier.side_effect = dataset_uris

    crucible = Crucible(overseer)

    prepared_datafile = crucible.prepare_datafile(refined_datafile)

    datafile.replicas = []  # Replicas are handled after crucible stage

    assert prepared_datafile == datafile


@responses.activate
def test_prepare_datafile_no_matching_dataset(
    caplog: pytest.LogCaptureFixture,
    connection: ConnectionConfig,
    crucible: Crucible,
    refined_datafile: RefinedDatafile,
    response_dict_not_found: Dict[str, Any],
) -> None:
    caplog.set_level(logging.WARNING)
    responses.get(
        urljoin(connection.api_template, "dataset"),
        status=200,
        json=(response_dict_not_found),
    )
    warning = "Unable to find the dataset associated with this datafile in MyTardis."

    assert crucible.prepare_datafile(refined_datafile) is None
    assert warning in caplog.text


@pytest.fixture(name="duplicate_dataset_response_dict")
def fixture_duplicate_dataset_response_dict(
    dataset_response_dict: Dict[str, Any],
) -> Dict[str, Any]:
    dataset = copy.deepcopy(dataset_response_dict["objects"][0])
    dataset["resource_uri"] = "/api/v1/dataset/2/"
    response_dict = copy.deepcopy(dataset_response_dict)
    response_dict["objects"].append(dataset)

    return response_dict


@pytest.mark.xfail
@responses.activate
def test_prepare_datafile_too_many_datasets(
    caplog: pytest.LogCaptureFixture,
    connection: ConnectionConfig,
    crucible: Crucible,
    refined_datafile: RefinedDatafile,
    duplicate_dataset_response_dict: Dict[str, Any],
    response_dict_not_found: Dict[str, Any],
) -> None:
    caplog.set_level(logging.WARNING)
    responses.get(
        urljoin(connection.api_template, "dataset"),
        match=[matchers.query_param_matcher({"identifiers": refined_datafile.dataset})],
        status=200,
        json=(duplicate_dataset_response_dict),
    )
    responses.get(
        urljoin(connection.api_template, "dataset"),
        status=200,
        json=(response_dict_not_found),
    )

    warning = (
        "Unable to uniquely identify the dataset associated with this datafile in "
        "MyTardis. Possible candidates are: "
    )

    assert crucible.prepare_datafile(refined_datafile) is None
    assert len(list(filter(lambda m: m.startswith(warning), caplog.messages))) == 1
