# pylint: disable=missing-function-docstring

"""Tests of the Smelter base class functions"""

import logging

import pytest

from src.helpers.config import (
    GeneralConfig,
    IntrospectionConfig,
    SchemaConfig,
    StorageConfig,
)
from src.smelters import Smelter

logger = logging.getLogger(__name__)
logger.propagate = True


def test_mytardis_setup_with_no_ids(mytardis_config):
    mytardis_setup_no_ids = mytardis_config
    mytardis_setup_no_ids.pop("objects_with_ids")
    Smelter.__abstractmethods__ = set()
    smelter_no_id = Smelter(
        general,
        default_schema,
        storage,
        mytardis_setup,
    )
    assert smelter_no_id.objects_with_ids is None


def test_inject_schema_from_default_value(
    smelter,
    raw_project_dictionary,
    project_schema,
):
    expected_output = raw_project_dictionary
    expected_output["schema"] = project_schema
    assert (
        smelter._inject_schema_from_default_value(
            raw_project_dictionary["name"], "project", raw_project_dictionary
        )
        == expected_output
    )


def test_inject_schema_from_default_value_with_no_default_schema(
    smelter, raw_project_dictionary
):
    smelter.default_schema = {}
    with pytest.raises(SanityCheckError):
        smelter._inject_schema_from_default_value(
            raw_project_dictionary["name"], "project", raw_project_dictionary
        )


def test_smelt_object_with_project(
    smelter,
    tidied_project_dictionary,
    raw_project_as_dict,
    project_parameters_as_dict,
):
    project_keys = [*PROJECT_KEYS]
    project_keys.append("persistent_id")
    project_keys.append("alternate_ids")
    test_project, parameters = smelter.smelt_object(
        tidied_project_dictionary, project_keys
    )
    assert raw_project_as_dict == test_project
    assert project_parameters_as_dict == parameters


def test_smelt_project(
    smelter,
    raw_project,
    tidied_project_dictionary,
    raw_project_parameterset,
):
    refined_project = smelter.smelt_project(tidied_project_dictionary)
    assert len(refined_project) == 2
    out_dict = refined_project[0]
    param_dict = refined_project[1]
    assert param_dict == raw_project_parameterset
    assert out_dict == raw_project


def test_smelt_project_no_metatdata(
    smelter,
    raw_project,
    tidied_project_dictionary,
    project_metadata,
):
    for key in project_metadata.keys():
        tidied_project_dictionary.pop(key)
    refined_project = smelter.smelt_project(tidied_project_dictionary)
    assert refined_project[0] == raw_project
    assert refined_project[1] is None


def test_smelt_project_no_projects(caplog, smelter):
    caplog.set_level(logging.WARNING)
    cleaned_dict = {}
    smelter.projects_enabled = False
    warning_str = (
        "MyTardis is not currently set up to use projects. Please check settings.py "
        "and ensure that the 'projects' app is enabled. This may require rerunning "
        "migrations."
    )
    test_values = smelter.smelt_project(cleaned_dict)
    assert warning_str in caplog.text
    assert test_values is None


def test_smelt_project_incomplete_dictionary(
    caplog,
    smelter,
    tidied_project_dictionary,
):
    caplog.set_level(logging.WARNING)
    tidied_project_dictionary.pop("name")
    warning_str = f"Unable to parse"
    refined_project = smelter.smelt_project(tidied_project_dictionary)
    assert warning_str in caplog.text
    assert refined_project is None


def test_smelt_project_no_schema(
    caplog,
    smelter,
    tidied_project_dictionary,
):
    smelter.default_schema = None
    tidied_project_dictionary.pop("schema")
    refined_project = smelter.smelt_project(tidied_project_dictionary)
    warning_str = "Unable to find default schemas and no schema provided"
    assert refined_project is None
    assert warning_str in caplog.text


def test_smelt_project_no_institution(
    caplog,
    smelter,
    tidied_project_dictionary,
):
    caplog.set_level(logging.WARNING)
    smelter.default_institution = None
    refined_project = smelter.smelt_project(tidied_project_dictionary)
    warning_str = "Unable to find default institution and no institution provided"
    assert warning_str in caplog.text
    assert refined_project is None


def test_smelt_object_with_experiment(
    smelter,
    tidied_experiment_dictionary,
    raw_experiment_as_dict,
    experiment_parameters_as_dict,
):
    experiment_keys = [*EXPERIMENT_KEYS]
    experiment_keys.append("persistent_id")
    experiment_keys.append("alternate_ids")
    test_experiment, parameters = smelter.smelt_object(
        tidied_experiment_dictionary, experiment_keys
    )
    assert raw_experiment_as_dict == test_experiment
    assert experiment_parameters_as_dict == parameters


def test_smelt_experiment(
    smelter,
    raw_experiment,
    tidied_experiment_dictionary,
    raw_experiment_parameterset,
):
    refined_experiment = smelter.smelt_experiment(tidied_experiment_dictionary)
    assert len(refined_experiment) == 2
    out_dict = refined_experiment[0]
    param_dict = refined_experiment[1]
    assert param_dict == raw_experiment_parameterset
    assert out_dict == raw_experiment


def test_smelt_experiment_no_metatdata(
    smelter,
    raw_experiment,
    tidied_experiment_dictionary,
    experiment_metadata,
):
    for key in experiment_metadata.keys():
        tidied_experiment_dictionary.pop(key)
    refined_experiment = smelter.smelt_experiment(tidied_experiment_dictionary)
    assert refined_experiment[0] == raw_experiment
    assert refined_experiment[1] is None


def test_smelt_experiment_incomplete_dictionary(
    caplog,
    smelter,
    tidied_experiment_dictionary,
):
    caplog.set_level(logging.WARNING)
    tidied_experiment_dictionary.pop("title")
    warning_str = f"Unable to parse"
    refined_experiment = smelter.smelt_experiment(tidied_experiment_dictionary)
    assert warning_str in caplog.text
    assert refined_experiment is None


def test_smelt_experiment_no_schema(
    caplog,
    smelter,
    tidied_experiment_dictionary,
):
    smelter.default_schema = None
    tidied_experiment_dictionary.pop("schema")
    refined_experiment = smelter.smelt_experiment(tidied_experiment_dictionary)
    warning_str = "Unable to find default schemas and no schema provided"
    assert refined_experiment is None
    assert warning_str in caplog.text


def test_smelt_object_with_dataset(
    smelter,
    tidied_dataset_dictionary,
    raw_dataset_as_dict,
    dataset_parameters_as_dict,
):
    dataset_keys = [*DATASET_KEYS]
    dataset_keys.append("persistent_id")
    dataset_keys.append("alternate_ids")
    test_dataset, parameters = smelter.smelt_object(
        tidied_dataset_dictionary, dataset_keys
    )
    assert raw_dataset_as_dict == test_dataset
    assert dataset_parameters_as_dict == parameters


def test_smelt_dataset(
    smelter,
    raw_dataset,
    tidied_dataset_dictionary,
    raw_dataset_parameterset,
):
    refined_dataset = smelter.smelt_dataset(tidied_dataset_dictionary)
    assert len(refined_dataset) == 2
    out_dict = refined_dataset[0]
    param_dict = refined_dataset[1]
    assert param_dict == raw_dataset_parameterset
    assert out_dict == raw_dataset


def test_smelt_dataset_no_metadata(
    smelter,
    raw_dataset,
    tidied_dataset_dictionary,
    dataset_metadata,
):
    for key in dataset_metadata.keys():
        tidied_dataset_dictionary.pop(key)
    refined_dataset = smelter.smelt_dataset(tidied_dataset_dictionary)
    assert refined_dataset[0] == raw_dataset
    assert refined_dataset[1] is None


def test_smelt_dataset_incomplete_dictionary(
    caplog,
    smelter,
    tidied_dataset_dictionary,
):
    caplog.set_level(logging.WARNING)
    tidied_dataset_dictionary.pop("description")
    warning_str = f"Unable to parse"
    refined_dataset = smelter.smelt_dataset(tidied_dataset_dictionary)
    assert warning_str in caplog.text
    assert refined_dataset is None


def test_smelt_dataset_no_schema(
    caplog,
    smelter,
    tidied_dataset_dictionary,
):
    smelter.default_schema = None
    tidied_dataset_dictionary.pop("schema")
    refined_dataset = smelter.smelt_dataset(tidied_dataset_dictionary)
    warning_str = "Unable to find default schemas and no schema provided"
    assert refined_dataset is None
    assert warning_str in caplog.text


def test_smelt_dataset_no_instrument(
    caplog,
    smelter,
    tidied_dataset_dictionary,
):
    caplog.set_level(logging.WARNING)
    tidied_dataset_dictionary.pop("instrument")
    warning_str = f"Unable to parse"
    refined_dataset = smelter.smelt_dataset(tidied_dataset_dictionary)
    assert warning_str in caplog.text
    assert refined_dataset is None


def test_create_replica(
    smelter,
    raw_datafile_dictionary,
    datafile_replica,
    storage_box,
):
    smelter.storage_box = storage_box
    assert datafile_replica == smelter._create_replica(
        raw_datafile_dictionary["relative_file_path"]
    )


def test_smelt_object_with_datafile(
    smelter,
    tidied_datafile_dictionary,
    raw_datafile_as_dict,
    datafile_parameters_as_dict,
):
    datafile_keys = [*DATAFILE_KEYS]
    datafile_keys.append("persistent_id")
    datafile_keys.append("alternate_ids")
    test_datafile, parameters = smelter.smelt_object(
        tidied_datafile_dictionary, datafile_keys
    )
    assert raw_datafile_as_dict == test_datafile
    assert datafile_parameters_as_dict == parameters


def test_smelt_datafile(
    smelter,
    raw_datafile,
    tidied_datafile_dictionary,
    raw_datafile_parameterset,
    storage_box,
):
    smelter.storage_box = storage_box
    out_dict = smelter.smelt_datafile(tidied_datafile_dictionary)
    assert out_dict == raw_datafile
    assert out_dict.parameter_sets == raw_datafile_parameterset


def test_smelt_datafile_no_metadata(
    smelter,
    raw_datafile,
    tidied_datafile_dictionary,
    storage_box,
    datafile_metadata,
):
    smelter.storage_box = storage_box
    raw_datafile.parameter_sets = None
    for key in datafile_metadata.keys():
        tidied_datafile_dictionary.pop(key)
    out_dict = smelter.smelt_datafile(tidied_datafile_dictionary)
    assert out_dict == raw_datafile
    assert out_dict.parameter_sets is None


def test_smelt_datafile_incomplete_dictionary(
    caplog,
    smelter,
    tidied_datafile_dictionary,
    storage_box,
):
    smelter.storage_box = storage_box
    caplog.set_level(logging.WARNING)
    tidied_datafile_dictionary.pop("filename")
    warning_str = f"Unable to parse"
    refined_datafile = smelter.smelt_datafile(tidied_datafile_dictionary)
    assert warning_str in caplog.text
    assert refined_datafile is None


def test_smelt_datafile_no_schema(
    caplog,
    smelter,
    tidied_datafile_dictionary,
    storage_box,
):
    smelter.storage_box = storage_box
    smelter.default_schema = None
    tidied_datafile_dictionary.pop("schema")
    refined_datafile = smelter.smelt_datafile(tidied_datafile_dictionary)
    warning_str = "Unable to find default schemas and no schema provided"
    assert refined_datafile is None
    assert warning_str in caplog.text


def test_smelt_datafile_no_replica(
    caplog, smelter, tidied_datafile_dictionary, storage_box
):
    smelter.storage_box = storage_box
    caplog.set_level(logging.WARNING)
    tidied_datafile_dictionary.pop("replicas")
    warning_str = f"Unable to parse"
    refined_datafile = smelter.smelt_datafile(tidied_datafile_dictionary)
    assert warning_str in caplog.text
    assert refined_datafile is None
