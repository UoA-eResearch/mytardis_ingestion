# pylint: disable=missing-function-docstring

"""Tests of the Smelter base class functions"""

import pysnooper

from src.smelters import Smelter

mytardis_setup = {
    "projects_enabled": True,
    "objects_with_ids": ["project", "experiment", "dataset", "institution"],
    "remote_directory": "//files.auckland.ac.nz/research/rescer202200001-test-dir/",
    "mount_directory": "/home/chris/GitRepos/mytardis_ingestion/tests/test_yaml_smelter/",
}


@pysnooper.snoop()
def test_tidy_up_dictionary_keys_with_good_inputs():
    parsed_dict = {"key1": "value1", "key2": "value2", "key3": "value3"}
    translation_dict = {"key1": "new_key1", "key2": "new_key2"}
    Smelter.__abstractmethods__ = set()
    smelter = Smelter(mytardis_setup)
    smelter.OBJECT_KEY_CONVERSION = translation_dict
    cleaned_dict = smelter.tidy_up_dictionary_keys(parsed_dict)
    assert cleaned_dict == {
        "new_key1": "value1",
        "new_key2": "value2",
        "key3": "value3",
    }


@pysnooper.snoop()
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


@pysnooper.snoop()
def test_split_dictionary_into_object_and_parameters():
    cleaned_dict = {
        "schema": "test schema",
        "name": "test",
        "key1": "value1",
        "pkey1": "pvalue1",
        "pkey2": "pvalue2",
    }
    object_keys = ["name", "key1"]
    object_dict = {"name": "test", "key1": "value1"}
    parameter_dict = {
        "schema": "test schema",
        "parameters": [
            {"name": "pkey1", "value": "pvalue1"},
            {"name": "pkey2", "value": "pvalue2"},
        ],
    }
    assert Smelter.smelt_object(object_keys, cleaned_dict) == (
        object_dict,
        parameter_dict,
    )
