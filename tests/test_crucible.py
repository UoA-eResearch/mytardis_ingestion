# pylint: disable=missing-module-docstring,missing-function-docstring
import copy
import logging
from urllib.parse import urljoin

import pytest
import responses
from responses import matchers

from src.blueprints.custom_data_types import URI
from src.blueprints.datafile import Datafile, RefinedDatafile
from src.blueprints.dataset import Dataset, RefinedDataset
from src.blueprints.experiment import Experiment, RefinedExperiment
from src.blueprints.project import Project, RefinedProject
from src.crucible.crucible import Crucible
from src.helpers.config import ConnectionConfig
from tests.fixtures.fixtures_dataclasses import refined_project


@responses.activate
def test_prepare_project(
    connection: ConnectionConfig,
    crucible: Crucible,
    refined_project: RefinedProject,
    project: Project,
    institution_response_dict,
    response_dict_not_found,
):
    url = urljoin(connection.api_template, "institution")
    responses.get(
        url,
        match=[
            matchers.query_param_matcher(
                {"identifiers": refined_project.institution[0]}
            ),
        ],
        status=200,
        json=(institution_response_dict),
    )
    responses.get(url, status=200, json=(response_dict_not_found))

    assert crucible.prepare_project(refined_project) == project


@responses.activate
def test_prepare_project_no_matching_institution(
    caplog: pytest.LogCaptureFixture,
    connection: ConnectionConfig,
    crucible: Crucible,
    refined_project: RefinedProject,
    response_dict_not_found,
):
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
    connection: ConnectionConfig,
    crucible: Crucible,
    refined_experiment: RefinedExperiment,
    experiment: Experiment,
    project_response_dict,
    response_dict_not_found,
    experiment_ids,
):
    if refined_experiment.projects:
        responses.get(
            urljoin(connection.api_template, "project"),
            match=[
                matchers.query_param_matcher(
                    {"identifiers": refined_experiment.projects[0]}
                )
            ],
            status=200,
            json=(project_response_dict),
        )
    responses.get(
        urljoin(connection.api_template, "project"),
        status=200,
        json=(response_dict_not_found),
    )
    assert crucible.prepare_experiment(refined_experiment) == experiment


@responses.activate
def test_prepare_experiment_no_matching_projects(
    caplog: pytest.LogCaptureFixture,
    connection: ConnectionConfig,
    crucible: Crucible,
    refined_experiment: RefinedExperiment,
    response_dict_not_found,
):
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
    connection: ConnectionConfig,
    crucible: Crucible,
    refined_dataset: RefinedDataset,
    dataset: Dataset,
    experiment_response_dict,
    instrument_response_dict,
    response_dict_not_found,
):
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
        match=[
            matchers.query_param_matcher({"identifiers": refined_dataset.instrument})
        ],
        status=200,
        json=(instrument_response_dict),
    )
    responses.get(
        urljoin(connection.api_template, "instrument"),
        status=200,
        json=(response_dict_not_found),
    )

    assert crucible.prepare_dataset(refined_dataset) == dataset


@responses.activate
def test_prepare_dataset_no_matching_experiments(
    caplog: pytest.LogCaptureFixture,
    connection: ConnectionConfig,
    crucible: Crucible,
    refined_dataset: RefinedDataset,
    response_dict_not_found,
):
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
    experiment_response_dict,
    response_dict_not_found,
):
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
    warning = "Unable to find the instrument associated with this dataset in MyTardis."

    assert crucible.prepare_dataset(refined_dataset) is None
    assert warning in caplog.text


@pytest.fixture
def duplicate_instrument_response_dict(instrument_response_dict):
    instrument = copy.deepcopy(instrument_response_dict["objects"][0])
    instrument["resource_uri"] = "/api/v1/instrument/2/"
    response_dict = copy.deepcopy(instrument_response_dict)
    response_dict["objects"].append(instrument)

    return response_dict


@responses.activate
def test_prepare_dataset_too_many_instruments(
    caplog: pytest.LogCaptureFixture,
    connection: ConnectionConfig,
    crucible: Crucible,
    refined_dataset: RefinedDataset,
    experiment_response_dict,
    duplicate_instrument_response_dict,
    response_dict_not_found,
):
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
        match=[
            matchers.query_param_matcher({"identifiers": refined_dataset.instrument})
        ],
        status=200,
        json=(duplicate_instrument_response_dict),
    )
    responses.get(
        urljoin(connection.api_template, "instrument"),
        status=200,
        json=(response_dict_not_found),
    )

    warning = "Unable to uniquely identify the instrument associated with the name or identifier provided. Possible candidates are: "

    assert crucible.prepare_dataset(refined_dataset) is None
    assert len(list(filter(lambda m: m.startswith(warning), caplog.messages))) == 1


@responses.activate
def test_prepare_datafile(
    connection: ConnectionConfig,
    crucible: Crucible,
    refined_datafile: RefinedDatafile,
    datafile: Datafile,
    dataset_response_dict,
    response_dict_not_found,
):
    responses.get(
        urljoin(connection.api_template, "dataset"),
        match=[matchers.query_param_matcher({"identifiers": refined_datafile.dataset})],
        status=200,
        json=(dataset_response_dict),
    )
    responses.get(
        urljoin(connection.api_template, "dataset"),
        status=200,
        json=(response_dict_not_found),
    )

    assert crucible.prepare_datafile(refined_datafile) == datafile


@responses.activate
def test_prepare_datafile_no_matching_dataset(
    caplog: pytest.LogCaptureFixture,
    connection: ConnectionConfig,
    crucible: Crucible,
    refined_datafile: RefinedDatafile,
    response_dict_not_found,
):
    caplog.set_level(logging.WARNING)
    responses.get(
        urljoin(connection.api_template, "dataset"),
        status=200,
        json=(response_dict_not_found),
    )
    warning = "Unable to find the dataset associated with this datafile in MyTardis."

    assert crucible.prepare_datafile(refined_datafile) is None
    assert warning in caplog.text


@pytest.fixture
def duplicate_dataset_response_dict(dataset_response_dict):
    dataset = copy.deepcopy(dataset_response_dict["objects"][0])
    dataset["resource_uri"] = "/api/v1/dataset/2/"
    response_dict = copy.deepcopy(dataset_response_dict)
    response_dict["objects"].append(dataset)

    return response_dict


@responses.activate
def test_prepare_datafile_too_many_datasets(
    caplog: pytest.LogCaptureFixture,
    connection: ConnectionConfig,
    crucible: Crucible,
    refined_datafile: RefinedDatafile,
    duplicate_dataset_response_dict,
    response_dict_not_found,
):
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

    warning = "Unable to uniquely identify the dataset associated with this datafile in MyTardis. Possible candidates are: "

    assert crucible.prepare_datafile(refined_datafile) is None
    assert len(list(filter(lambda m: m.startswith(warning), caplog.messages))) == 1
