# import pytest

import logging
import shutil
from pathlib import Path

import mock
import pytest

from .conftest import (
    config_dict,
    datadir,
    project_dict_as_read_from_yaml,
    raw_project_dictionary,
)

logger = logging.getLogger(__name__)
logger.propagate = True

from src.smelters import YAMLSmelter

test_malformed_ingestion_dict_no_object = {"project_files": "some-data"}

test_malformed_ingestion_dict_too_many_objects = {
    "project_name": "test_name",
    "experiment_name": "test_name_2",
}


@pytest.fixture
def YAML_config_dict(config_dict):
    configuration = config_dict
    configuration["projects_enabled"] = True
    configuration["objects_with_ids"] = [
        "dataset",
        "experiment",
        "facility",
        "instrument",
        "project",
        "institution",
    ]
    return configuration


def test_get_objects_in_input_file(datadir, YAML_config_dict):
    input_file = Path(datadir / "test_project.yaml")
    smelter = YAMLSmelter(YAML_config_dict)
    assert smelter.get_objects_in_input_file(input_file) == ("project",)


@pytest.mark.dependency()
def test_read_file(datadir, YAML_config_dict, project_dict_as_read_from_yaml):
    input_file = Path(datadir / "test_project.yaml")
    smelter = YAMLSmelter(YAML_config_dict)
    print(smelter.read_file(input_file))
    print(project_dict_as_read_from_yaml)
    assert smelter.read_file(input_file) == (project_dict_as_read_from_yaml,)


@pytest.mark.dependency(depends=["test_read_file"])
def test_read_file_exceptions_log_error(caplog, datadir, YAML_config_dict):
    caplog.set_level(logging.ERROR)
    smelter = YAMLSmelter(YAML_config_dict)
    input_file = Path(datadir / "no_such_file.dat")
    with pytest.raises(FileNotFoundError):
        _ = smelter.read_file(input_file)
        error_str = f"Failed to read file {input_file}."
        assert error_str in caplog.text
        assert "FileNotFoundError" in caplog.text


@pytest.mark.dependency(depends=["test_read_file"])
def test_tidy_up_metadata_keys(datadir, YAML_config_dict):
    input_file = Path(datadir / "test_project.yaml")
    smelter = YAMLSmelter(YAML_config_dict)
    parsed_dict = smelter.read_file(input_file)
    object_type = smelter.get_object_from_dictionary(parsed_dict[0])
    cleaned_dict = smelter._tidy_up_metadata_keys(parsed_dict[0], object_type)
    assert cleaned_dict["project_my_test_key_1"] == "Test Value"
    assert cleaned_dict["project_my_test_key_2"] == "Test Value 2"


@pytest.mark.dependency(depends=["test_read_file", "test_tidy_up_metadata_keys"])
def test_rebase_file_path(datadir, YAML_config_dict):
    input_file = Path(datadir / "test_datafile.yaml")
    smelter = YAMLSmelter(YAML_config_dict)
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


def test_get_file_type_for_input_files(YAML_config_dict):
    smelter = YAMLSmelter(YAML_config_dict)
    assert smelter.get_file_type_for_input_files() == "*.yaml"


@mock.patch("src.smelters.yaml_smelter.YAMLSmelter.read_file")
def test_malformed_ingestion_dict_no_object_logs_warning(
    mock_read_file, caplog, YAML_config_dict
):
    mock_read_file.return_value = [
        test_malformed_ingestion_dict_no_object,
    ]
    caplog.set_level(logging.WARNING)
    smelter = YAMLSmelter(YAML_config_dict)
    file_path = Path("/home/test/test.dat")
    object_types = smelter.get_objects_in_input_file(file_path)
    warning_str = f"File {file_path} was not recognised as a MyTardis ingestion file"
    assert warning_str in caplog.text
    assert object_types == (None,)


@mock.patch("src.smelters.yaml_smelter.YAMLSmelter.read_file")
def test_malformed_ingestion_dict_too_many_objects_logs_warning(
    mock_read_file, caplog, YAML_config_dict
):
    mock_read_file.return_value = [
        test_malformed_ingestion_dict_too_many_objects,
    ]
    caplog.set_level(logging.WARNING)
    smelter = YAMLSmelter(YAML_config_dict)
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
def test_expand_datafile_entry(datadir, YAML_config_dict):
    input_file = Path(datadir / "test_datafile.yaml")
    smelter = YAMLSmelter(YAML_config_dict)
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


@pytest.mark.xfail()
@pytest.mark.dependency(depends=["test_expand_datafile_entry"])
@mock.patch("pathlib.Path.iterdir")
def test_expand_datafile_entry_for_directories(mock_iterdir, datadir, YAML_config_dict):
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
    smelter = YAMLSmelter(YAML_config_dict)
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
    assert set(test_processed_file_list) == set(file_list)
