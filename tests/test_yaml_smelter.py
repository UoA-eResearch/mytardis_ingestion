# import pytest

import logging
import shutil
from pathlib import Path

import mock
import pytest
from pytest import fixture

logger = logging.getLogger(__name__)
logger.propagate = True

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

test_malformed_ingestion_dict_no_object = {"project_files": "some-data"}

test_malformed_ingestion_dict_too_many_objects = {
    "project_name": "test_name",
    "experiment_name": "test_name_2",
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
def test_read_file_exceptions_log_error(caplog, datadir):
    caplog.set_level(logging.ERROR)
    smelter = YAMLSmelter(config_dict)
    input_file = Path(datadir / "no_such_file.dat")
    with pytest.raises(FileNotFoundError):
        _ = smelter.read_file(input_file)
        error_str = f"Failed to read file {input_file}."
        assert error_str in caplog.text
        assert "FileNotFoundError" in caplog.text


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


def test_get_file_type_for_input_files():
    smelter = YAMLSmelter(config_dict)
    assert smelter.get_file_type_for_input_files() == "*.yaml"


@mock.patch("src.smelters.yaml_smelter.YAMLSmelter.read_file")
def test_malformed_ingestion_dict_no_object_logs_warning(mock_read_file, caplog):
    mock_read_file.return_value = [
        test_malformed_ingestion_dict_no_object,
    ]
    caplog.set_level(logging.WARNING)
    smelter = YAMLSmelter(config_dict)
    file_path = Path("/home/test/test.dat")
    object_types = smelter.get_objects_in_input_file(file_path)
    warning_str = f"File {file_path} was not recognised as a MyTardis ingestion file"
    assert warning_str in caplog.text
    assert object_types == (None,)


@mock.patch("src.smelters.yaml_smelter.YAMLSmelter.read_file")
def test_malformed_ingestion_dict_too_many_objects_logs_warning(mock_read_file, caplog):
    mock_read_file.return_value = [
        test_malformed_ingestion_dict_too_many_objects,
    ]
    caplog.set_level(logging.WARNING)
    smelter = YAMLSmelter(config_dict)
    file_path = Path("/home/test/test.dat")
    object_types = smelter.get_objects_in_input_file(file_path)
    warning_str = (
        f"Malformed MyTardis ingestion file, {file_path}. Please ensure that "
        "sections are properly delimited with '---' and that each section is "
        "defined for one object type only i.e. 'project', 'experiment', "
        "'dataset' or 'datafile'."
    )
    assert warning_str in caplog.text
    assert object_types == (None,)


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
    test_processed_file_list = [
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
    assert file_list == test_processed_file_list


@pytest.mark.dependency(depends=["test_expand_datafile_entry"])
@mock.patch("pathlib.Path.iterdir")
def test_expand_datafile_entry_for_directories(mock_iterdir, datadir):
    iter_dir_return_list = [
        Path(datadir / "test_data.dat"),
        Path(datadir / "test_data2.dat"),
    ]
    mock_iterdir.return_value = iter_dir_return_list
    test_parsed_dict = {
        "datafiles": {
            "files": [
                {"name": Path(datadir)},
            ],
            "dataset_id": ["Test-dataset"],
        },
    }
    smelter = YAMLSmelter(config_dict)
    smelter.mount_dir = Path(datadir)
    file_list = smelter.expand_datafile_entry(test_parsed_dict)
    print(file_list)
    test_processed_file_list = [
        {
            "dataset": "Test-dataset",
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
    assert test_processed_file_list == file_list
