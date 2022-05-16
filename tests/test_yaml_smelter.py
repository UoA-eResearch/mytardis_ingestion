# import pytest

from src.smelters import YAMLSmelter
import logging
import shutil
from pathlib import Path

import mock
import pytest
from pytest import fixture

from .conftest import config_dict, datadir, raw_project_dictionary

logger = logging.getLogger(__name__)
logger.propagate = True


test_malformed_ingestion_dict_no_object = {"project_files": "some-data"}

test_malformed_ingestion_dict_too_many_objects = {
    "project_name": "test_name",
    "experiment_name": "test_name_2",
}


@fixture
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


@fixture
def smelter(YAML_config_dict):
    return YAMLSmelter(YAML_config_dict)


@fixture
def project_dictionary_from_yaml_file():
    return {
        "project_name": "Test Project",
        "project_id": "Project_1",
        "alternate_ids": [
            "Test_Project",
            "Project_Test_1",
        ],
        "description": "A test project for the purposes of testing",
        "lead_researcher": "upi001",
        "admin_groups": [
            "Test_Group_1",
        ],
        "admin_users": [
            "upi002",
            "upi003",
        ],
        "metadata": {"My Test Key 1": "Test Value", "My Test Key 2": "Test Value 2"},
    }


@pytest.mark.dependency()
def test_tidy_up_metadata_keys(smelter, raw_project_dictionary):
    cleaned_dict = smelter._tidy_up_metadata_keys(
        raw_project_dictionary, "project")
    assert cleaned_dict["project_my_test_key_1"] == "Test Value"
    assert cleaned_dict["project_my_test_key_2"] == "Test Value 2"


@mock.patch("src.smelters.yaml_smelter.YAMLSmelter.read_file")
def test_get_objects_in_input_file(
    mock_read_file,
    YAML_config_dict,
    smelter,
    project_dictionary_from_yaml_file,
):
    mock_read_file.return_value = [project_dictionary_from_yaml_file]
    assert smelter.get_objects_in_input_file(project_dictionary_from_yaml_file) == (
        "project",
    )


@mock.patch("src.smelters.yaml_smelter.YAMLSmelter.read_file")
def test_get_objects_in_input_file_with_no_objects(
    mock_read_file,
    caplog,
    YAML_config_dict,
    smelter,
    project_dictionary_from_yaml_file,
):
    caplog.set_level(logging.WARNING)
    test_dictionary = project_dictionary_from_yaml_file
    test_dictionary.pop("project_name")
    mock_read_file.return_value = [test_dictionary]
    warning_str = (
        f"File {Path('Test_file_path')} was not recognised as a MyTardis ingestion file"
    )
    assert smelter.get_objects_in_input_file(Path("Test_file_path")) == (None,)
    assert warning_str in caplog.text


@mock.patch("src.smelters.yaml_smelter.YAMLSmelter.read_file")
def test_get_objects_in_input_file_with_two_objects(
    mock_read_file,
    caplog,
    YAML_config_dict,
    smelter,
    project_dictionary_from_yaml_file,
):
    caplog.set_level(logging.WARNING)
    test_dictionary = project_dictionary_from_yaml_file
    test_dictionary["datafiles"] = "Test Datafile"
    mock_read_file.return_value = [test_dictionary]
    warning_str = (
        f"Malformed MyTardis ingestion file, {Path('Test_file_path')}. Please ensure that "
        "sections are properly delimited with '---' and that each section is "
        "defined for one object type only i.e. 'project', 'experiment', "
        "'dataset' or 'datafile'."
    )
    assert smelter.get_objects_in_input_file(Path("Test_file_path")) == (None,)
    assert warning_str in caplog.text


@pytest.mark.dependency()
def test_read_file(datadir, smelter, project_dictionary_from_yaml_file):
    input_file = Path(datadir / "test_project.yaml")
    assert smelter.read_file(input_file) == (
        project_dictionary_from_yaml_file,)


@pytest.mark.dependency(depends=["test_read_file"])
def test_read_file_exceptions_log_error(caplog, datadir, smelter):
    caplog.set_level(logging.ERROR)
    input_file = Path(datadir / "no_such_file.dat")
    with pytest.raises(FileNotFoundError):
        _ = smelter.read_file(input_file)
        error_str = f"Failed to read file {input_file}."
        assert error_str in caplog.text
        assert "FileNotFoundError" in caplog.text


"""@pytest.mark.dependency(depends=["test_read_file", "test_tidy_up_metadata_keys"])
def test_rebase_file_path(datadir, smelter):
    input_file = Path(datadir / "test_datafile.yaml")
    parsed_dict = smelter.read_file(input_file)
    cleaned_dict = smelter.rebase_file_path(parsed_dict[0])
    test_dict = {
        "datafiles": {
            "dataset_id": ["Test-dataset"],
            "files": [
                {
                    "metadata": {
                        "My Test Key 1": "Test Value",
                        "My Test Key 2": "Test Value 2",
                    },
                    "name": Path("/target/path/test_data.dat"),
                },
                {"name": Path("/target/path/test_data2.dat")},
            ],
        }
    }
    assert cleaned_dict == test_dict"""


@pytest.mark.xfail
@pytest.mark.dependency(depends=["test_read_file", "test_tidy_up_metadata_keys"])
def test_expand_datafile_entry(datadir, smelter):
    input_file = Path(datadir / "test_datafile.yaml")
    smelter.source_dir = Path(datadir)
    cleaned_dict = smelter.read_file(input_file)
    # smelter.source_dir = Path("/source/path")
    # cleaned_dict = smelter.rebase_file_path(parsed_dict[0])
    file_list = smelter.expand_datafile_entry(cleaned_dict[0])
    test_processed_file_list = [
        {
            "dataset": "Test-dataset",
            "My Test Key 1": "Test Value",
            "My Test Key 2": "Test Value 2",
            "filename": "test_data.dat",
            "md5sum": "0d32909e86e422d04a053d1ba26a990e",
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
def test_expand_datafile_entry_for_directories(mock_iterdir, datadir, smelter):
    iter_dir_return_list = [
        Path(datadir / "test_data.dat"),
        Path(datadir / "test_data2.dat"),
    ]
    mock_iterdir.return_value = iter_dir_return_list
    smelter.source_dir = Path(datadir)
    test_parsed_dict = {
        "datafiles": {
            "files": [
                {"name": Path(datadir)},
            ],
            "dataset_id": ["Test-dataset"],
        },
    }
    file_list = smelter.expand_datafile_entry(test_parsed_dict)
    test_processed_file_list = [
        {
            "dataset": "Test-dataset",
            "filename": "test_data.dat",
            "md5sum": "0d32909e86e422d04a053d1ba26a990e",
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


def test_get_object_from_dictionary(smelter):
    project_dict = {"project_name": "Name"}
    experiment_dict = {"experiment_name": "Name"}
    dataset_dict = {"dataset_name": "Name"}
    datafile_dict = {"datafiles": "Namee"}
    assert smelter.get_object_from_dictionary(project_dict) == "project"
    assert smelter.get_object_from_dictionary(experiment_dict) == "experiment"
    assert smelter.get_object_from_dictionary(dataset_dict) == "dataset"
    assert smelter.get_object_from_dictionary(datafile_dict) == "datafile"


def test_get_input_file_paths(datadir, smelter):
    def as_posix(path: Path): return path.as_posix()
    file_list = list(map(
        as_posix,
        [
            Path(datadir / "test_datafile.yaml"),
            Path(datadir / "Pathtest_datafile.yml"),
            Path(datadir / "test_dataset.yaml"),
            Path(datadir / "test_dataset.yml"),
            Path(datadir / "test_experiment.yaml"),
            Path(datadir / "test_experiment.yml"),
            Path(datadir / "test_mixed.yaml"),
            Path(datadir / "test_mixed.yml"),
            Path(datadir / "test_project.yaml"),
            Path(datadir / "test_project.yml")
        ]
    ))
    path_list = list(map(as_posix, smelter.get_input_file_paths(datadir)))
    assert len(path_list) == len(file_list)
    assert path_list.sort() == file_list.sort()
