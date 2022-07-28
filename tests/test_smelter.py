# pylint: disable=missing-function-docstring

"""Tests of the Smelter base class functions"""

import logging
from pathlib import Path

import mock
import pytest

from src.helpers import SanityCheckError
from src.helpers.config import (
    GeneralConfig,
    IntrospectionConfig,
    SchemaConfig,
    StorageConfig,
)
from src.smelters import Smelter

logger = logging.getLogger(__name__)
logger.propagate = True


def test_tidy_up_dictionary_keys_with_good_inputs(smelter):
    parsed_dict = {"key1": "value1", "key2": "value2", "key3": "value3"}
    # Note we have no object_type in the Smelter Base class so use None as object type key
    translation_dict = {None: {"key1": "new_key1", "key2": "new_key2"}}
    smelter.OBJECT_KEY_CONVERSION = translation_dict
    cleaned_dict = smelter.tidy_up_dictionary_keys(parsed_dict)
    assert cleaned_dict == (
        None,
        {
            "new_key1": "value1",
            "new_key2": "value2",
            "key3": "value3",
        },
    )


def test_tidy_up_dictionary_keys_with_no_translation_dict(caplog, smelter):
    caplog.set_level(logging.WARNING)
    parsed_dict = {"key1": "value1", "key2": "value2", "key3": "value3"}
    cleaned_dict = smelter.tidy_up_dictionary_keys(parsed_dict)
    assert "Unable to find None in OBJECT_KEY_CONVERSION dictionary" in caplog.text
    assert cleaned_dict == (
        None,
        {"key1": "value1", "key2": "value2", "key3": "value3"},
    )


def test_mytardis_setup_with_no_ids(
    general: GeneralConfig,
    default_schema: SchemaConfig,
    storage: StorageConfig,
    mytardis_setup: IntrospectionConfig,
):
    mytardis_setup.objects_with_ids = None
    Smelter.__abstractmethods__ = set()
    smelter_no_id = Smelter(
        general,
        default_schema,
        storage,
        mytardis_setup,
    )
    assert smelter_no_id.objects_with_ids is None


def test_parse_groups_and_users_from_separate_access():
    cleaned_dict = {
        "admin_users": ["usr_a", "usr_b", "usr_a"],
        "admin_groups": ["grp_a"],
        "read_users": ["usr_c", "usr_d"],
        "read_groups": ["grp_b", "grp_c", "grp_c"],
        "download_users": ["usr_d", "usr_f"],
        "download_groups": ["grp_c", "grp_d", "grp_f"],
        "sensitive_users": ["usr_f", "usr_e"],
        "sensitive_groups": ["grp_e", "grp_f"],
        "another_key": "value",
    }
    users, groups = Smelter.parse_groups_and_users_from_separate_access(cleaned_dict)
    users = sorted(users)
    groups = sorted(groups)
    test_users = [
        ("usr_a", True, True, True),
        ("usr_b", True, True, True),
        ("usr_c", False, False, False),
        ("usr_d", False, True, False),
        ("usr_e", False, False, True),
        ("usr_f", False, True, True),
    ]
    test_groups = [
        ("grp_a", True, True, True),
        ("grp_b", False, False, False),
        ("grp_c", False, True, False),
        ("grp_d", False, True, False),
        ("grp_e", False, False, True),
        ("grp_f", False, True, True),
    ]
    assert users == test_users
    assert groups == test_groups
    assert cleaned_dict == {"another_key": "value"}


def test_parse_groups_and_users_from_separate_access_no_admin_but_pi():
    cleaned_dict = {
        "principal_investigator": "upi001",
        "read_users": ["upi002"],
    }
    users, _ = Smelter.parse_groups_and_users_from_separate_access(cleaned_dict)
    users = sorted(users)
    assert users == [
        ("upi001", True, True, True),
        ("upi002", False, False, False),
    ]


def test_parse_groups_and_users_from_separate_access_pi_adds_to_admin():
    cleaned_dict = {
        "admin_users": ["upi001"],
        "principal_investigator": "upi002",
    }
    users, _ = Smelter.parse_groups_and_users_from_separate_access(cleaned_dict)
    users = sorted(users)
    assert users == [
        ("upi001", True, True, True),
        ("upi002", True, True, True),
    ]


@mock.patch("src.smelters.smelter.Smelter._tidy_up_metadata_keys")
def test_split_dictionary_into_object_and_parameters(
    mock_smelter_tidy_up_metadata_keys, smelter
):
    cleaned_dict = {
        "schema": "test schema",
        "name": "test",
        "key1": "value1",
        "pkey1": "pvalue1",
        "pkey2": "pvalue2",
    }
    mock_smelter_tidy_up_metadata_keys.return_value = cleaned_dict
    object_keys = ["name", "key1"]
    object_dict = {"name": "test", "key1": "value1"}
    parameter_dict = {
        "schema": "test schema",
        "parameters": [
            {"name": "pkey1", "value": "pvalue1"},
            {"name": "pkey2", "value": "pvalue2"},
        ],
    }
    translation_dict = {None: {}}
    smelter.OBJECT_KEY_CONVERSION = translation_dict
    assert smelter._smelt_object(object_keys, cleaned_dict) == (
        object_dict,
        parameter_dict,
    )


def test_set_access_control():
    combined_names = [
        "user1",
        "user2",
        "user3",
        "user4",
    ]
    download_names = [
        "user2",
        "user4",
    ]
    sensitive_names = [
        "user3",
        "user4",
    ]
    expected_output = [
        ("user1", False, False, False),
        ("user2", False, True, False),
        ("user3", False, False, True),
        ("user4", False, True, True),
    ]
    assert (
        Smelter.set_access_controls(combined_names, download_names, sensitive_names)
        == expected_output
    )


def test_set_access_control_strings():
    combined_names = "user1"
    download_names = "user1"
    sensitive_names = "user1"
    expected_output = [
        ("user1", False, True, True),
    ]
    assert (
        Smelter.set_access_controls(combined_names, download_names, sensitive_names)
        == expected_output
    )


def test_verify_project_with_bad_dictionary(
    caplog,
    smelter,
    raw_project_dictionary,
):
    caplog.set_level(logging.WARNING)
    sanity_check = smelter._verify_project(raw_project_dictionary)
    missing_keys = sorted(
        [
            "institution",
            "schema",
        ]
    )
    warning_str = (
        "Incomplete data for Project creation\n"
        f"cleaned_dict: {raw_project_dictionary}\n"
        f"missing keys: {missing_keys}"
    )
    assert sanity_check == False
    assert warning_str in caplog.text


def test_verify_project_with_good_dictionary(
    smelter,
    raw_project_dictionary,
):
    project_dict = raw_project_dictionary
    project_dict["schema"] = "https://test.mytardis.nectar.auckland.ac.nz/project/v1"
    project_dict["institution"] = "Test Institution"
    assert smelter._verify_project(project_dict) == True


def test_smelt_project_no_projects(
    caplog,
    general: GeneralConfig,
    default_schema: SchemaConfig,
    storage: StorageConfig,
    mytardis_setup: IntrospectionConfig,
):
    caplog.set_level(logging.WARNING)
    mytardis_setup.projects_enabled = False
    cleaned_dict = {}
    Smelter.__abstractmethods__ = set()
    smelter = Smelter(
        general,
        default_schema,
        storage,
        mytardis_setup,
    )
    warning_str = (
        "MyTardis is not currently set up to use projects. Please check settings.py "
        "and ensure that the 'projects' app is enabled. This may require rerunning "
        "migrations."
    )
    test_values = smelter.smelt_project(cleaned_dict)
    assert warning_str in caplog.text
    assert test_values == (None, None)


def test_inject_schema_from_default_value(smelter, raw_project_dictionary):
    expected_output = raw_project_dictionary
    expected_output["schema"] = "https://test.mytardis.nectar.auckland.ac.nz/project/v1"
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


@mock.patch("src.smelters.smelter.Smelter.get_object_from_dictionary")
@mock.patch("src.smelters.smelter.Smelter._tidy_up_metadata_keys")
def test_smelt_project(
    mock_tidy_up_metadata_keys,
    mock_get_object_from_dictionary,
    smelter,
    raw_project_dictionary,
    tidied_project_dictionary,
):
    mock_get_object_from_dictionary.return_value = "project"
    mock_tidy_up_metadata_keys.return_value = tidied_project_dictionary
    out_dict, param_dict = smelter.smelt_project(raw_project_dictionary)
    assert param_dict == {
        "schema": "https://test.mytardis.nectar.auckland.ac.nz/project/v1",
        "parameters": [
            {
                "name": "project_my_test_key_1",
                "value": "Test Value",
            },
            {
                "name": "project_my_test_key_2",
                "value": "Test Value 2",
            },
        ],
    }
    assert out_dict == {
        "users": [
            ("upi001", True, True, True),
            ("upi002", True, True, True),
            ("upi003", True, True, True),
        ],
        "groups": [("Test_Group_1", True, True, True)],
        "alternate_ids": ["Test_Project", "Project_Test_1"],
        "description": "A test project for the purposes of testing",
        "name": "Test Project",
        "persistent_id": "Project_1",
        "principal_investigator": "upi001",
        "institution": ["Test Institution"],
    }


@mock.patch("src.smelters.smelter.Smelter.get_object_from_dictionary")
@mock.patch("src.smelters.smelter.Smelter._tidy_up_metadata_keys")
def test_smelt_project_no_schema(
    mock_tidy_up_metadata_keys,
    mock_get_object_from_dictionary,
    caplog,
    smelter,
    raw_project_dictionary,
    tidied_project_dictionary,
):
    caplog.set_level(logging.WARNING)
    mock_get_object_from_dictionary.return_value = "project"
    test_dictionary = tidied_project_dictionary
    test_dictionary.pop("schema")
    mock_tidy_up_metadata_keys.return_value = test_dictionary
    smelter.default_schema = {}
    out_dict, param_dict = smelter.smelt_project(raw_project_dictionary)
    warning_str = "Unable to find default project schema and no schema provided"
    assert warning_str in caplog.text
    assert param_dict == None
    assert out_dict == None


@mock.patch("src.smelters.smelter.Smelter.get_object_from_dictionary")
@mock.patch("src.smelters.smelter.Smelter._tidy_up_metadata_keys")
def test_smelt_project_no_institution(
    mock_tidy_up_metadata_keys,
    mock_get_object_from_dictionary,
    caplog,
    smelter,
    raw_project_dictionary,
    tidied_project_dictionary,
):
    caplog.set_level(logging.WARNING)
    mock_get_object_from_dictionary.return_value = "project"
    test_dictionary = tidied_project_dictionary
    test_dictionary.pop("institution")
    mock_tidy_up_metadata_keys.return_value = test_dictionary
    smelter.default_institution = None
    out_dict, param_dict = smelter.smelt_project(raw_project_dictionary)
    warning_str = "Unable to find default institution and no institution provided"
    assert warning_str in caplog.text
    assert param_dict == None
    assert out_dict == None


def test_verify_experiment_with_bad_dictionary(
    caplog,
    smelter,
    raw_experiment_dictionary,
):
    caplog.set_level(logging.WARNING)
    sanity_check = smelter._verify_experiment(raw_experiment_dictionary)
    missing_keys = sorted(
        [
            "schema",
        ]
    )
    warning_str = (
        "Incomplete data for Experiment creation\n"
        f"cleaned_dict: {raw_experiment_dictionary}\n"
        f"missing keys: {missing_keys}"
    )
    assert sanity_check == False
    assert warning_str in caplog.text


def test_verify_experiment_with_good_dictionary(
    smelter,
    raw_experiment_dictionary,
):
    experiment_dict = raw_experiment_dictionary
    experiment_dict[
        "schema"
    ] = "https://test.mytardis.nectar.auckland.ac.nz/experiment/v1"
    assert smelter._verify_experiment(experiment_dict) == True


@mock.patch("src.smelters.smelter.Smelter.get_object_from_dictionary")
@mock.patch("src.smelters.smelter.Smelter._tidy_up_metadata_keys")
def test_smelt_experiment(
    mock_tidy_up_metadata_keys,
    mock_get_object_from_dictionary,
    smelter,
    raw_experiment_dictionary,
    tidied_experiment_dictionary,
):
    mock_get_object_from_dictionary.return_value = "experiment"
    mock_tidy_up_metadata_keys.return_value = tidied_experiment_dictionary
    out_dict, param_dict = smelter.smelt_experiment(raw_experiment_dictionary)
    assert param_dict == {
        "schema": "https://test.mytardis.nectar.auckland.ac.nz/experiment/v1",
        "parameters": [
            {
                "name": "experiment_my_test_key_1",
                "value": "Test Value",
            },
            {
                "name": "experiment_my_test_key_2",
                "value": "Test Value 2",
            },
        ],
    }
    assert out_dict == {
        "alternate_ids": ["Test_Experiment", "Experiment_Test_1"],
        "description": "A test experiment for the purposes of testing",
        "title": "Test Experiment",
        "persistent_id": "Experiment_1",
        "projects": ["Project_1", "Test_Project"],
    }


@mock.patch("src.smelters.smelter.Smelter.get_object_from_dictionary")
@mock.patch("src.smelters.smelter.Smelter._tidy_up_metadata_keys")
def test_smelt_experiment_no_schema(
    mock_tidy_up_metadata_keys,
    mock_get_object_from_dictionary,
    caplog,
    smelter,
    raw_experiment_dictionary,
    tidied_experiment_dictionary,
):
    caplog.set_level(logging.WARNING)
    mock_get_object_from_dictionary.return_value = "experiment"
    test_dict = tidied_experiment_dictionary
    test_dict.pop("schema")
    mock_tidy_up_metadata_keys.return_value = test_dict
    smelter.default_schema = {}
    out_dict, param_dict = smelter.smelt_experiment(raw_experiment_dictionary)
    warning_str = "Unable to find default experiment schema and no schema provided"
    assert warning_str in caplog.text
    assert param_dict == None
    assert out_dict == None


def test_verify_dataset_with_bad_dictionary(
    caplog,
    smelter,
    raw_dataset_dictionary,
):
    caplog.set_level(logging.WARNING)
    sanity_check = smelter._verify_dataset(raw_dataset_dictionary)
    missing_keys = sorted(
        [
            "schema",
        ]
    )
    warning_str = (
        "Incomplete data for Dataset creation\n"
        f"cleaned_dict: {raw_dataset_dictionary}\n"
        f"missing keys: {missing_keys}"
    )
    assert sanity_check == False
    assert warning_str in caplog.text


def test_verify_dataset_with_good_dictionary(
    smelter,
    raw_dataset_dictionary,
):
    dataset_dict = raw_dataset_dictionary
    dataset_dict["schema"] = "https://test.mytardis.nectar.auckland.ac.nz/dataset/v1"
    assert smelter._verify_dataset(dataset_dict) == True


@mock.patch("src.smelters.smelter.Smelter.get_object_from_dictionary")
@mock.patch("src.smelters.smelter.Smelter._tidy_up_metadata_keys")
def test_smelt_dataset(
    mock_tidy_up_metadata_keys,
    mock_get_object_from_dictionary,
    smelter,
    raw_dataset_dictionary,
    tidied_dataset_dictionary,
):
    mock_get_object_from_dictionary.return_value = "dataset"
    mock_tidy_up_metadata_keys.return_value = tidied_dataset_dictionary
    out_dict, param_dict = smelter.smelt_dataset(raw_dataset_dictionary)
    assert param_dict == {
        "schema": "https://test.mytardis.nectar.auckland.ac.nz/dataset/v1",
        "parameters": [
            {
                "name": "dataset_my_test_key_1",
                "value": "Test Value",
            },
            {
                "name": "dataset_my_test_key_2",
                "value": "Test Value 2",
            },
        ],
    }
    assert out_dict == {
        "alternate_ids": ["Test_Dataset", "Dataset_Test_1"],
        "description": "Test Dataset",
        "instrument": "Instrument_1",
        "persistent_id": "Dataset_1",
        "experiments": ["Experiment_1", "Test_Experiment"],
    }


@mock.patch("src.smelters.smelter.Smelter.get_object_from_dictionary")
@mock.patch("src.smelters.smelter.Smelter._tidy_up_metadata_keys")
def test_smelt_dataset_no_schema(
    mock_tidy_up_metadata_keys,
    mock_get_object_from_dictionary,
    caplog,
    smelter,
    raw_dataset_dictionary,
    tidied_dataset_dictionary,
):
    caplog.set_level(logging.WARNING)
    mock_get_object_from_dictionary.return_value = "dataset"
    test_dictionary = tidied_dataset_dictionary
    test_dictionary.pop("schema")
    mock_tidy_up_metadata_keys.return_value = test_dictionary
    smelter.default_schema = {}
    out_dict, param_dict = smelter.smelt_dataset(raw_dataset_dictionary)
    warning_str = "Unable to find default dataset schema and no schema provided"
    assert warning_str in caplog.text
    assert param_dict == None
    assert out_dict == None


def test_create_replica(smelter, raw_datafile_dictionary):
    expected_output = raw_datafile_dictionary
    expected_output["replicas"] = [
        {
            "uri": "test_data.dat",
            "storage_box": "Test_storage_box",
            "protocol": "file",
        },
    ]
    raw_datafile_dictionary["file_path"] = Path("test_data.dat")
    assert expected_output == smelter._create_replica(raw_datafile_dictionary)


def test_verify_datafile_with_bad_dictionary(
    caplog,
    smelter,
    tidied_datafile_dictionary,
):
    caplog.set_level(logging.WARNING)
    test_dictionary = tidied_datafile_dictionary
    test_dictionary.pop("schema")
    sanity_check = smelter._verify_datafile(test_dictionary)
    missing_keys = sorted(
        [
            "schema",
        ]
    )
    warning_str = (
        "Incomplete data for Datafile creation\n"
        f"cleaned_dict: {test_dictionary}\n"
        f"missing keys: {missing_keys}"
    )
    assert sanity_check == False
    assert warning_str in caplog.text


def test_verify_datafile_with_good_dictionary(
    smelter,
    tidied_datafile_dictionary,
):
    dataset_dict = tidied_datafile_dictionary
    assert smelter._verify_datafile(dataset_dict) == True


@mock.patch("src.smelters.smelter.Smelter._create_replica")
@mock.patch("src.smelters.smelter.Smelter.get_object_from_dictionary")
@mock.patch("src.smelters.smelter.Smelter._tidy_up_metadata_keys")
def test_smelt_datafile(
    mock_tidy_up_metadata_keys,
    mock_get_object_from_dictionary,
    mock_create_replica,
    smelter,
    raw_datafile_dictionary,
    tidied_datafile_dictionary,
    preconditioned_datafile_dictionary,
):
    mock_get_object_from_dictionary.return_value = "datafile"
    mock_tidy_up_metadata_keys.return_value = preconditioned_datafile_dictionary
    mock_create_replica.return_value = tidied_datafile_dictionary
    out_dict = smelter.smelt_datafile(raw_datafile_dictionary)
    print(out_dict)
    assert out_dict == {
        "dataset": ["Dataset_1"],
        "filename": "test_data.dat",
        "md5sum": "0d32909e86e422d04a053d1ba26a990e",
        "size": 52428800,
        "parameter_sets": {
            "schema": "https://test.mytardis.nectar.auckland.ac.nz/datafile/v1",
            "parameters": [
                {
                    "name": "datafile_my_test_key_1",
                    "value": "Test Value",
                },
                {
                    "name": "datafile_my_test_key_2",
                    "value": "Test Value 2",
                },
            ],
        },
        "replicas": [
            {
                "uri": "test_data.dat",
                "location": "Test_storage_box",
                "protocol": "file",
            },
        ],
    }


@mock.patch("src.smelters.smelter.Smelter.get_object_from_dictionary")
@mock.patch("src.smelters.smelter.Smelter._tidy_up_metadata_keys")
def test_smelt_datafile_no_schema(
    mock_tidy_up_metadata_keys,
    mock_get_object_from_dictionary,
    caplog,
    smelter,
    raw_datafile_dictionary,
    tidied_datafile_dictionary,
):
    caplog.set_level(logging.WARNING)
    mock_get_object_from_dictionary.return_value = "datafile"
    test_dict = tidied_datafile_dictionary
    test_dict.pop("schema")
    mock_tidy_up_metadata_keys.return_value = tidied_datafile_dictionary
    smelter.default_schema = {}
    out_dict = smelter.smelt_datafile(raw_datafile_dictionary)
    warning_str = "Unable to find default datafile schema and no schema provided"
    assert warning_str in caplog.text
    assert out_dict is None


@mock.patch("src.smelters.smelter.Smelter.get_object_from_dictionary")
@mock.patch("src.smelters.smelter.Smelter._tidy_up_metadata_keys")
def test_smelt_datafile_no_replica(
    mock_tidy_up_metadata_keys,
    mock_get_object_from_dictionary,
    caplog,
    smelter,
    raw_datafile_dictionary,
    tidied_datafile_dictionary,
):
    caplog.set_level(logging.WARNING)
    test_dictionary = raw_datafile_dictionary
    test_dictionary.pop("file_path")
    mock_get_object_from_dictionary.return_value = "datafile"
    mock_tidy_up_metadata_keys.return_value = tidied_datafile_dictionary
    out_dict = smelter.smelt_datafile(test_dictionary)
    filename = test_dictionary["filename"]
    warning_str = f"Unable to create file replica for {filename}"
    assert warning_str in caplog.text
    assert out_dict is None
