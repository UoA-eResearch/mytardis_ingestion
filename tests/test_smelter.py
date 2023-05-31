# pylint: disable=missing-function-docstring

"""Tests of the Smelter base class functions"""

import logging
from typing import Any

import pytest
from pydantic import AnyUrl

from src.blueprints.common_models import ParameterSet
from src.blueprints.datafile import RawDatafile, RefinedDatafile
from src.blueprints.dataset import RawDataset, RefinedDataset
from src.blueprints.experiment import RawExperiment, RefinedExperiment
from src.blueprints.project import RawProject, RefinedProject
from src.smelters.smelter import Smelter

logger = logging.getLogger(__name__)
logger.propagate = True


@pytest.mark.dependency()
def test_extract_parameters(
    smelter: Smelter,
    raw_project: RawProject,
    project_schema: AnyUrl,
    raw_project_parameterset: ParameterSet,
) -> None:
    assert (
        smelter.extract_parameters(project_schema, raw_project)
        == raw_project_parameterset
    )
    Smelter.clear()


def test_extract_parameters_metadata_none(
    smelter: Smelter,
    raw_project: RawProject,
    project_schema: AnyUrl,
) -> None:
    raw_project.metadata = None

    assert smelter.extract_parameters(project_schema, raw_project) is None
    Smelter.clear()


def test_extract_parameters_object_schema_none(
    smelter: Smelter,
    raw_project: RawProject,
    project_schema: AnyUrl,
    raw_project_parameterset: ParameterSet,
) -> None:
    raw_project.object_schema = None
    assert (
        smelter.extract_parameters(project_schema, raw_project)
        == raw_project_parameterset
    )
    Smelter.clear()


def test_smelt_project(
    smelter: Smelter,
    raw_project: RawProject,
    refined_project: RefinedProject,
    raw_project_parameterset: ParameterSet,
) -> None:
    assert smelter.smelt_project(raw_project) == (
        refined_project,
        raw_project_parameterset,
    )
    Smelter.clear()


def test_smelt_project_projects_disabled(
    caplog: pytest.LogCaptureFixture,
    smelter: Smelter,
    raw_project: RawProject,
) -> None:
    caplog.set_level(logging.WARNING)
    error_str = (
        "MyTardis is not currently set up to use projects. Please check settings.py "
    )
    smelter.overseer.mytardis_setup.projects_enabled = False

    assert smelter.smelt_project(raw_project) is None
    assert error_str in caplog.text
    Smelter.clear()


def test_smelt_project_object_use_default_schema(
    smelter: Smelter,
    raw_project: RawProject,
    refined_project: RefinedProject,
    raw_project_parameterset: ParameterSet,
) -> None:
    raw_project.object_schema = None

    result = smelter.smelt_project(raw_project)

    assert result is not None
    (_, parameterset) = result
    if parameterset:
        assert parameterset.parameter_schema == smelter.default_schema.project
    assert result == (refined_project, raw_project_parameterset)
    Smelter.clear()


def test_smelt_project_use_default_institution(
    smelter: Smelter,
    raw_project: RawProject,
    refined_project: RefinedProject,
    raw_project_parameterset: ParameterSet,
) -> None:
    raw_project.institution = None

    result = smelter.smelt_project(raw_project)

    assert result is not None
    (project, _) = result
    assert project.institution[0] == smelter.default_institution
    assert result == (
        refined_project,
        raw_project_parameterset,
    )
    Smelter.clear()


def test_smelt_project_no_institution(
    caplog: Any,
    smelter: Smelter,
    raw_project: RawProject,
) -> None:
    caplog.set_level(logging.WARNING)
    error_str = "Unable to find default institution and no institution provided"
    raw_project.institution = None
    smelter.default_institution = None

    assert smelter.smelt_project(raw_project) is None
    assert error_str in caplog.text
    Smelter.clear()


def test_smelt_project_no_schema(
    caplog: Any,
    smelter: Smelter,
    raw_project: RawProject,
) -> None:
    caplog.set_level(logging.WARNING)
    error_str = "Unable to find default project schema and no schema provided"
    raw_project.object_schema = None
    smelter.default_schema.project = None

    assert smelter.smelt_project(raw_project) is None
    assert error_str in caplog.text
    Smelter.clear()


def test_smelt_experiment(
    smelter: Smelter,
    raw_experiment: RawExperiment,
    refined_experiment: RefinedExperiment,
    raw_experiment_parameterset: ParameterSet,
) -> None:
    assert smelter.smelt_experiment(raw_experiment) == (
        refined_experiment,
        raw_experiment_parameterset,
    )
    Smelter.clear()


def test_smelt_experiment_projects_enabled(
    caplog: pytest.LogCaptureFixture,
    smelter: Smelter,
    raw_experiment: RawExperiment,
) -> None:
    caplog.set_level(logging.WARNING)
    error_str = "Projects enabled in MyTardis and no projects provided to link this experiment to. Experiment provided "  # pylint: disable=line-too-long
    raw_experiment.projects = None
    if not smelter.overseer.mytardis_setup.projects_enabled:
        smelter.overseer.mytardis_setup.projects_enabled = True

    assert smelter.smelt_experiment(raw_experiment) is None
    assert error_str in caplog.text
    Smelter.clear()


def test_smelt_experiment_object_use_default_schema(
    smelter: Smelter,
    raw_experiment: RawExperiment,
    refined_experiment: RefinedExperiment,
    raw_experiment_parameterset: ParameterSet,
) -> None:
    raw_experiment.object_schema = None

    result = smelter.smelt_experiment(raw_experiment)

    assert result is not None
    (_, parameterset) = result
    if parameterset:
        assert parameterset.parameter_schema == smelter.default_schema.experiment
    assert result == (refined_experiment, raw_experiment_parameterset)
    Smelter.clear()


def test_smelt_experiment_no_schema(
    caplog: pytest.LogCaptureFixture,
    smelter: Smelter,
    raw_experiment: RawExperiment,
) -> None:
    caplog.set_level(logging.WARNING)
    error_str = "Unable to find default experiment schema and no schema provided"
    raw_experiment.object_schema = None
    smelter.default_schema.experiment = None

    assert smelter.smelt_experiment(raw_experiment) is None
    assert error_str in caplog.text
    Smelter.clear()


def test_smelt_experiment_use_default_institution(
    smelter: Smelter,
    raw_experiment: RawExperiment,
    refined_experiment: RefinedExperiment,
    raw_experiment_parameterset: ParameterSet,
) -> None:
    raw_experiment.institution_name = None

    result = smelter.smelt_experiment(raw_experiment)

    assert result is not None
    (experiment, _) = result
    assert experiment.institution_name == smelter.default_institution
    assert result == (refined_experiment, raw_experiment_parameterset)
    Smelter.clear()


def test_smelt_experiment_no_institution(
    caplog: Any,
    smelter: Smelter,
    raw_experiment: RawExperiment,
) -> None:
    caplog.set_level(logging.WARNING)
    error_str = "Unable to find default institution and no institution provided"
    raw_experiment.institution_name = None
    smelter.default_institution = None

    assert smelter.smelt_experiment(raw_experiment) is None
    assert error_str in caplog.text
    Smelter.clear()


def test_smelt_dataset(
    smelter: Smelter,
    raw_dataset: RawDataset,
    refined_dataset: RefinedDataset,
    raw_dataset_parameterset: ParameterSet,
) -> None:
    assert smelter.smelt_dataset(raw_dataset) == (
        refined_dataset,
        raw_dataset_parameterset,
    )
    Smelter.clear()


def test_smelt_dataset_use_default_schema(
    smelter: Smelter,
    raw_dataset: RawDataset,
    refined_dataset: RefinedDataset,
    raw_dataset_parameterset: ParameterSet,
) -> None:
    raw_dataset.object_schema = None

    result = smelter.smelt_dataset(raw_dataset)

    assert result is not None
    (_, parameterset) = result
    if parameterset:
        assert parameterset.parameter_schema == smelter.default_schema.dataset
    assert result == (refined_dataset, raw_dataset_parameterset)
    Smelter.clear()


def test_smelt_dataset_no_schema(
    caplog: pytest.LogCaptureFixture,
    smelter: Smelter,
    raw_dataset: RawDataset,
) -> None:
    caplog.set_level(logging.WARNING)
    error_str = "Unable to find default dataset schema and no schema provided"
    raw_dataset.object_schema = None
    smelter.default_schema.dataset = None

    assert smelter.smelt_dataset(raw_dataset) is None
    assert error_str in caplog.text
    Smelter.clear()


def test_smelt_datafile(
    smelter: Smelter,
    raw_datafile: RawDatafile,
    refined_datafile: RefinedDatafile,
) -> None:
    assert smelter.smelt_datafile(raw_datafile) == refined_datafile
    Smelter.clear()


def test_smelt_datafile_use_default_schema(
    smelter: Smelter,
    raw_datafile: RawDatafile,
    refined_datafile: RefinedDatafile,
) -> None:
    raw_datafile.object_schema = None

    result = smelter.smelt_datafile(raw_datafile)

    assert result is not None
    if result.parameter_sets:
        assert result.parameter_sets.parameter_schema == smelter.default_schema.datafile
    assert result == refined_datafile
    Smelter.clear()


def test_smelt_datafile_no_schema(
    caplog: pytest.LogCaptureFixture,
    smelter: Smelter,
    raw_datafile: RawDatafile,
) -> None:
    caplog.set_level(logging.WARNING)
    error_str = "Unable to find default datafile schema and no schema provided"
    raw_datafile.object_schema = None
    smelter.default_schema.datafile = None

    assert smelter.smelt_datafile(raw_datafile) is None
    assert error_str in caplog.text
    Smelter.clear()
