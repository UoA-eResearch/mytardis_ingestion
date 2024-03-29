# pylint: disable=missing-module-docstring,missing-function-docstring
# nosec assert_used
# flake8: noqa S101

import copy
import logging
from datetime import datetime
from typing import Any, Dict
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


@pytest.mark.xfail
@responses.activate
def test_prepare_project(
    connection: ConnectionConfig,
    crucible: Crucible,
    refined_project: RefinedProject,
    project: Project,
    institution_response_dict: Dict[str, Any],
    response_dict_not_found: Dict[str, Any],
) -> None:
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
    responses.get(url, status=200, json=response_dict_not_found)
    assert crucible.prepare_project(refined_project) == project


@pytest.mark.xfail
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


@pytest.mark.xfail
@responses.activate
def test_prepare_experiment(
    connection: ConnectionConfig,
    crucible: Crucible,
    refined_experiment: RefinedExperiment,
    experiment: Experiment,
    project_response_dict: Dict[str, Any],
    response_dict_not_found: Dict[str, Any],
) -> None:
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


@pytest.mark.xfail
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


@pytest.mark.xfail
@responses.activate
def test_prepare_dataset(
    connection: ConnectionConfig,
    crucible: Crucible,
    refined_dataset: RefinedDataset,
    dataset: Dataset,
    experiment_response_dict: Dict[str, Any],
    instrument_response_dict: Dict[str, Any],
    response_dict_not_found: Dict[str, Any],
) -> None:
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


@pytest.mark.xfail
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


@pytest.mark.xfail
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
    warning = "Unable to find the instrument associated with this dataset in MyTardis."

    assert crucible.prepare_dataset(refined_dataset) is None
    assert warning in caplog.text


@pytest.fixture(name="duplicate_instrument_response_dict")
def fixture_duplicate_instrument_response_dict(
    instrument_response_dict: Dict[str, Any]
) -> Dict[str, Any]:
    instrument = copy.deepcopy(instrument_response_dict["objects"][0])
    instrument["resource_uri"] = "/api/v1/instrument/2/"
    response_dict = copy.deepcopy(instrument_response_dict)
    response_dict["objects"].append(instrument)

    return response_dict


@pytest.mark.xfail
@responses.activate
def test_prepare_dataset_too_many_instruments(
    caplog: pytest.LogCaptureFixture,
    connection: ConnectionConfig,
    crucible: Crucible,
    refined_dataset: RefinedDataset,
    experiment_response_dict: Dict[str, Any],
    duplicate_instrument_response_dict: Dict[str, Any],
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

    warning = (
        "Unable to uniquely identify the instrument associated with the "
        "name or identifier provided. Possible candidates are: "
    )

    assert crucible.prepare_dataset(refined_dataset) is None
    assert len(list(filter(lambda m: m.startswith(warning), caplog.messages))) == 1


@pytest.mark.xfail
@responses.activate
def test_prepare_datafile(
    mocker: Any,
    datetime_now: datetime,
    connection: ConnectionConfig,
    crucible: Crucible,
    refined_datafile: RefinedDatafile,
    datafile: Datafile,
    dataset_response_dict: Dict[str, Any],
    response_dict_not_found: Dict[str, Any],
) -> None:
    mock_date = mocker.patch("src.crucible.crucible.datetime")
    mock_date.now.return_value = datetime_now
    responses.get(
        urljoin(connection.api_template, "dataset"),
        match=[matchers.query_param_matcher({"identifiers": refined_datafile.dataset})],
        status=200,
        json=(dataset_response_dict),
    )
    responses.get(
        urljoin(connection.api_template, "dataset/1/"),
        status=200,
        json=(dataset_response_dict),
    )
    responses.get(
        urljoin(connection.api_template, "dataset"),
        status=200,
        json=(response_dict_not_found),
    )

    assert crucible.prepare_datafile(refined_datafile) == datafile


@pytest.mark.xfail
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
