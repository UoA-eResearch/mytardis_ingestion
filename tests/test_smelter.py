# pylint: disable=missing-function-docstring

"""Tests of the Smelter base class functions"""

import logging

import mock

from src.smelters import Smelter

logger = logging.getLogger(__name__)
logger.propagate = True

mytardis_setup = {
    "projects_enabled": True,
    "objects_with_ids": ["project", "experiment", "dataset", "institution"],
    "remote_directory": "/remote/path",
    "mount_directory": "/mount/path",
    "storage_box": "MyTest Storage",
    "default_institution": "Test Institution",
    "hostname": "https://test.mytardis.nectar.auckland.ac.nz",
    "default_schema": {
        "project": "https://test.mytardis.nectar.auckland.ac.nz/project/v1",
        "experiment": "https://test.mytardis.nectar.auckland.ac.nz/experiment/v1",
        "dataset": "https://test.mytardis.nectar.auckland.ac.nz/dataset/v1",
        "datafile": "https://test.mytardis.nectar.auckland.ac.nz/datafile/v1",
    },
}

mytardis_setup_no_projects = {
    "projects_enabled": False,
    "objects_with_ids": ["experiment", "dataset", "institution"],
    "remote_directory": "/remote/path",
    "mount_directory": "/mount/path",
    "storage_box": "MyTest Storage",
}

mytardis_setup_no_ids = {
    "projects_enabled": True,
    "remote_directory": "/remote/path",
    "mount_directory": "/mount/path",
    "storage_box": "MyTest Storage",
}

test_project_dict = {
    "name": "Test Project",
    "description": "Test project for testing",
    "principal_investigator": "Test User",
    "persistent_id": "TestID",
    "alternate_ids": [
        "Alt ID 1",
        "Alt ID 2",
    ],
    "metadata1": "Test parameter 1",
    "metadata2": "Test parameter 2",
}


def test_tidy_up_dictionary_keys_with_good_inputs():
    parsed_dict = {"key1": "value1", "key2": "value2", "key3": "value3"}
    # Note we have no object_type in the Smelter Base class so use None as object type key
    translation_dict = {None: {"key1": "new_key1", "key2": "new_key2"}}
    Smelter.__abstractmethods__ = set()
    smelter = Smelter(mytardis_setup)  # noqa
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


def test_tidy_up_dictionary_keys_with_no_translation_dict(caplog):
    caplog.set_level(logging.WARNING)
    parsed_dict = {"key1": "value1", "key2": "value2", "key3": "value3"}
    Smelter.__abstractmethods__ = set()
    smelter = Smelter(mytardis_setup)
    cleaned_dict = smelter.tidy_up_dictionary_keys(parsed_dict)
    assert "Unable to find None in OBJECT_KEY_CONVERSION dictionary" in caplog.text
    assert cleaned_dict == (
        None,
        {"key1": "value1", "key2": "value2", "key3": "value3"},
    )


def test_mytardis_setup_with_no_ids():
    Smelter.__abstractmethods__ = set()
    smelter = Smelter(mytardis_setup_no_ids)
    assert smelter.objects_with_ids == []


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


@mock.patch("src.smelters.smelter.Smelter._tidy_up_metadata_keys")
def test_split_dictionary_into_object_and_parameters(
    mock_smelter_tidy_up_metadata_keys,
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
    Smelter.__abstractmethods__ = set()
    smelter = Smelter(mytardis_setup)  # noqa
    smelter.OBJECT_KEY_CONVERSION = translation_dict
    assert smelter._smelt_object(object_keys, cleaned_dict) == (
        object_dict,
        parameter_dict,
    )


def test_smelt_project_no_projects(caplog):
    caplog.set_level(logging.WARNING)
    cleaned_dict = {}
    Smelter.__abstractmethods__ = set()
    smelter = Smelter(mytardis_setup_no_projects)
    warning_str = (
        "MyTardis is not currently set up to use projects. Please check settings.py "
        "and ensure that the 'projects' app is enabled. This may require rerunning "
        "migrations."
    )
    test_values = smelter.smelt_project(cleaned_dict)
    assert warning_str in caplog.text
    assert test_values == (None, None)


"""def test_smelt_project(datadir):
    # TODO: Fix this test
    input_file = Path(datadir / "test_project.yaml")
    smelter = YAMLSmelter(config_dict)
    parsed_dict = smelter.read_file(input_file)
    out_dict, param_dict = smelter.smelt_project(parsed_dict[0])
    assert param_dict == {
        "schema": "http://dummy.test/proj_schema",
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
            ("csea004", True, True, True),
            ("user2", True, True, True),
        ],
        "groups": [("Test_Group_1", True, True, True)],
        "alternate_ids": ["Test_Project", "Project_Test_1"],
        "description": "A test project for the purposes of testing the YAMLSmelter class",
        "name": "Test Project",
        "persistent_id": "Project_1",
        "principal_investigator": "csea004",
    }"""
