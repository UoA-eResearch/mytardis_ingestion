# pylint: disable=missing-function-docstring

"""Tests of the Smelter base class functions"""

import logging
from pydantic import AnyUrl

import pytest
from src.blueprints.common_models import ParameterSet
from src.blueprints.experiment import RawExperiment, RefinedExperiment
from src.blueprints.project import RawProject, RefinedProject

from src.smelters import Smelter

logger = logging.getLogger(__name__)
logger.propagate = True


@pytest.mark.dependency()
def test_extract_parameters(
    smelter: Smelter,
    raw_project: RawProject,
    project_schema,
    raw_project_parameterset: ParameterSet,
):
    assert (
        smelter.extract_parameters(project_schema, raw_project)
        == raw_project_parameterset
    )


def test_extract_parameters_metadata_none(
    smelter: Smelter, raw_project: RawProject, project_schema
):
    raw_project.metadata = None

    assert smelter.extract_parameters(project_schema, raw_project) is None


def test_extract_parameters_object_schema_none(
    smelter: Smelter,
    raw_project: RawProject,
    project_schema,
    raw_project_parameterset: ParameterSet,
):
    raw_project.object_schema = None
    assert (
        smelter.extract_parameters(project_schema, raw_project)
        == raw_project_parameterset
    )


@pytest.mark.dependency(depends=["test_extract_parameters"])
def test_smelt_project(
    smelter: Smelter,
    raw_project: RawProject,
    refined_project: RefinedProject,
    raw_project_parameterset: ParameterSet,
):
    assert smelter.smelt_project(raw_project) == (
        refined_project,
        raw_project_parameterset,
    )


def test_smelt_project_projects_disabled(
    caplog, smelter: Smelter, raw_project: RawProject
):
    caplog.set_level(logging.WARNING)
    error_str = (
        "MyTardis is not currently set up to use projects. Please check settings.py "
    )
    smelter.projects_enabled = False

    assert smelter.smelt_project(raw_project) is None
    assert error_str in caplog.text


def test_smelt_project_object_use_default_schema(
    smelter: Smelter,
    raw_project: RawProject,
    refined_project: RefinedProject,
    raw_project_parameterset: ParameterSet,
):
    raw_project.object_schema = None

    result = smelter.smelt_project(raw_project)

    assert result is not None
    (_, parameterset) = result
    assert parameterset.parameter_schema == smelter.default_schema.project
    assert result == (refined_project, raw_project_parameterset)


def test_smelt_project_no_schema(caplog, smelter: Smelter, raw_project: RawProject):
    caplog.set_level(logging.WARNING)
    error_str = "Unable to find default project schema and no schema provided"
    raw_project.object_schema = None
    smelter.default_schema.project = None

    assert smelter.smelt_project(raw_project) is None
    assert error_str in caplog.text


def test_smelt_project_use_default_institution(
    smelter: Smelter,
    raw_project: RawProject,
    refined_project: RefinedProject,
    raw_project_parameterset: ParameterSet,
):
    raw_project.institution = None

    result = smelter.smelt_project(raw_project)

    assert result is not None
    (project, _) = result
    assert project.institution[0] == smelter.default_institution
    assert result == (
        refined_project,
        raw_project_parameterset,
    )


def test_smelt_project_no_institution(
    caplog, smelter: Smelter, raw_project: RawProject
):
    caplog.set_level(logging.WARNING)
    error_str = "Unable to find default institution and no institution provided"
    raw_project.institution = None
    smelter.default_institution = None

    assert smelter.smelt_project(raw_project) is None
    assert error_str in caplog.text


@pytest.mark.dependency(depends=["test_extract_parameters"])
def test_smelt_experiment(
    smelter: Smelter,
    raw_experiment: RawExperiment,
    refined_experiment: RefinedExperiment,
    raw_experiment_parameterset: ParameterSet,
):
    assert smelter.smelt_experiment(raw_experiment) == (
        refined_experiment,
        raw_experiment_parameterset,
    )


def test_smelt_experiment_projects_enabled(
    caplog, smelter: Smelter, raw_experiment: RawExperiment
):
    caplog.set_level(logging.WARNING)
    error_str = "Projects enabled in MyTardis and no projects provided to link this experiment to. Experiment provided "
    raw_experiment.projects = None
    smelter.projects_enabled = True

    assert smelter.smelt_experiment(raw_experiment) is None
    assert error_str in caplog.text


def test_smelt_experiment_object_use_default_schema(
    smelter: Smelter,
    raw_experiment: RawExperiment,
    refined_experiment: RefinedExperiment,
    raw_experiment_parameterset: ParameterSet,
):
    raw_experiment.object_schema = None

    result = smelter.smelt_experiment(raw_experiment)

    assert result is not None
    (_, parameterset) = result
    assert parameterset.parameter_schema == smelter.default_schema.experiment
    assert result == (refined_experiment, raw_experiment_parameterset)


def test_smelt_experiment_no_schema(
    caplog, smelter: Smelter, raw_experiment: RawExperiment
):
    caplog.set_level(logging.WARNING)
    error_str = "Unable to find default experiment schema and no schema provided"
    raw_experiment.object_schema = None
    smelter.default_schema.experiment = None

    assert smelter.smelt_experiment(raw_experiment) is None
    assert error_str in caplog.text


def test_smelt_experiment_use_default_institution(
    smelter: Smelter,
    raw_experiment: RawExperiment,
    refined_experiment: RefinedExperiment,
    raw_experiment_parameterset: ParameterSet,
):
    raw_experiment.institution_name = None

    result = smelter.smelt_experiment(raw_experiment)

    assert result is not None
    (experiment, _) = result
    assert experiment.institution_name == smelter.default_institution
    assert result == (refined_experiment, raw_experiment_parameterset)


def test_smelt_experiment_no_institution(
    caplog, smelter: Smelter, raw_experiment: RawExperiment
):
    caplog.set_level(logging.WARNING)
    error_str = "Unable to find default institution and no institution provided"
    raw_experiment.institution_name = None
    smelter.default_institution = None

    assert smelter.smelt_experiment(raw_experiment) is None
    assert error_str in caplog.text
