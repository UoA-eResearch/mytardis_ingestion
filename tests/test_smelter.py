# pylint: disable=missing-function-docstring

"""Tests of the Smelter base class functions"""

import logging

import pytest
from src.blueprints.common_models import ParameterSet
from src.blueprints.experiment import RawExperiment, RefinedExperiment
from src.blueprints.project import RawProject, RefinedProject

from src.smelters import Smelter

logger = logging.getLogger(__name__)
logger.propagate = True


@pytest.mark.dependency()
def test_extract_parameters(
    smelter: Smelter, raw_project: RawProject, raw_project_parameterset: ParameterSet
):
    assert smelter.extract_parameters(raw_project) == raw_project_parameterset


def test_extract_parameters_metadata_none(smelter: Smelter, raw_project: RawProject):
    raw_project.metadata = None
    assert smelter.extract_parameters(raw_project) is None


def test_extract_parameters_object_schema_none(
    smelter: Smelter, raw_project: RawProject
):
    raw_project.object_schema = None
    assert smelter.extract_parameters(raw_project) is None


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


def test_smelt_project_object_schema_none(
    smelter: Smelter,
    raw_project: RawProject,
    refined_project: RefinedProject,
    raw_project_parameterset: ParameterSet,
):
    raw_project.object_schema = None

    assert smelter.smelt_project(raw_project) == (
        refined_project,
        raw_project_parameterset,
    )


def test_smelt_project_no_schema(caplog, smelter: Smelter, raw_project: RawProject):
    caplog.set_level(logging.WARNING)
    error_str = "Unable to find default project schema and no schema provided"
    raw_project.object_schema = None
    smelter.default_schema.project = None

    assert smelter.smelt_project(raw_project) is None
    assert error_str in caplog.text


def test_smelt_project_institution_none(
    smelter: Smelter,
    raw_project: RawProject,
    refined_project: RefinedProject,
    raw_project_parameterset: ParameterSet,
):
    raw_project.institution = None

    assert smelter.smelt_project(raw_project) == (
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
