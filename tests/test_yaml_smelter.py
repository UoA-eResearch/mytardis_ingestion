# import pytest

import shutil
from pathlib import Path

import pytest
from pytest import fixture

from src.smelters import YAMLSmelter

config_dict = {
    "username": "Test_User",
    "api_key": "Test_API_Key",
    "hostname": "https://test.mytardis.nectar.auckland.ac.nz",
    "verify_certificate": True,
    "proxy_http": "http://myproxy.com",
    "proxy_https": "http://myproxy.com",
    "projects_enabled": True,
    "objects_with_ids": ["project"],
    "remote_directory": "/remote/path",
    "mount_directory": "/mount/path",
    "storage_box": "Test_storage_box",
    "default_schema": {
        "project": "http://dummy.test/proj_schema",
    },
}


@fixture
def datadir(tmpdir, request):
    """
    Fixture responsible for searching a folder with the same name of test
    module and, if available, moving all contents to a temporary directory so
    tests can use them freely.
    """
    filename = request.module.__file__
    test_dir = Path(filename).with_suffix("")

    if test_dir.is_dir():
        for source in test_dir.glob("*"):
            shutil.copy(source, tmpdir)

    return tmpdir


def test_get_objects_in_input_file(datadir):
    input_file = Path(datadir / "test_project.yaml")
    smelter = YAMLSmelter(config_dict)
    assert smelter.get_objects_in_input_file(input_file) == ("project",)


@pytest.mark.dependency()
def test_read_file(datadir):
    input_file = Path(datadir / "test_project.yaml")
    smelter = YAMLSmelter(config_dict)
    assert smelter.read_file(input_file) == (
        {
            "project_name": "Test Project",
            "project_id": "Project_1",
            "alternate_ids": [
                "Test_Project",
                "Project_Test_1",
            ],
            "description": (
                "A test project for the purposes" " of testing the YAMLSmelter class"
            ),
            "lead_researcher": "csea004",
            "admin_groups": ["Test_Group_1"],
            "admin_users": ["csea004", "user2"],
            "metadata": {
                "My Test Key 1": "Test Value",
                "My Test Key 2": "Test Value 2",
            },
        },
    )


@pytest.mark.dependency(depends=["test_read_file"])
def test_tidy_up_metadata_keys(datadir):
    input_file = Path(datadir / "test_project.yaml")
    smelter = YAMLSmelter(config_dict)
    parsed_dict = smelter.read_file(input_file)
    object_type = smelter.get_object_from_dictionary(parsed_dict[0])
    cleaned_dict = smelter._tidy_up_metadata_keys(parsed_dict[0], object_type)
    assert cleaned_dict["project_my_test_key_1"] == "Test Value"
    assert cleaned_dict["project_my_test_key_2"] == "Test Value 2"


@pytest.mark.dependency(depends=["test_read_file", "test_tidy_up_metadata_keys"])
def test_smelt_project(datadir):
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
    }


@pytest.mark.dependency(depends=["test_read_file", "test_tidy_up_metadata_keys"])
def test_rebase_file_path(datadir):
    input_file = Path(datadir / "test_datafile.yaml")
    smelter = YAMLSmelter(config_dict)
    parsed_dict = smelter.read_file(input_file)
    cleaned_dict = smelter.rebase_file_path(parsed_dict[0])
    test_dict = {
        "datafiles": {
            "dataset_id": ["Test-dataset"],
            "files": [
                {
                    "metadata": {"key1": "value1", "key2": "value2"},
                    "name": Path("/mount/path/test_data.dat"),
                },
                {"name": Path("/mount/path/test_data2.dat")},
            ],
        }
    }
    assert cleaned_dict == test_dict


@pytest.mark.dependency(
    depends=["test_read_file", "test_tidy_up_metadata_keys", "test_rebase_file_path"]
)
def test_expand_datafile_entry(datadir):
    input_file = Path(datadir / "test_datafile.yaml")
    smelter = YAMLSmelter(config_dict)
    smelter.mount_dir = Path(datadir)
    parsed_dict = smelter.read_file(input_file)
    cleaned_dict = smelter.rebase_file_path(parsed_dict[0])
    file_list = smelter.expand_datafile_entry(cleaned_dict)
    test_list = [
        {
            "dataset": "Test-dataset",
            "key1": "value1",
            "key2": "value2",
            "filename": "test_data.dat",
            "md5sum": "0d32909e86e422d04a053d1ba26a990e",
            "full_path": Path(datadir / "test_data.dat"),
            "replicas": [
                {
                    "uri": "test_data.dat",
                    "location": "Test_storage_box",
                    "protocol": "file",
                },
            ],
            "size": 52428800,
        },
        {
            "dataset": "Test-dataset",
            "filename": "test_data2.dat",
            "md5sum": "457a252fffb56e95c74bc99dfc3830c2",
            "full_path": Path(datadir / "test_data2.dat"),
            "replicas": [
                {
                    "uri": "test_data2.dat",
                    "location": "Test_storage_box",
                    "protocol": "file",
                },
            ],
            "size": 1048576,
        },
    ]

    assert file_list == test_list
