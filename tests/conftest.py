import shutil
from pathlib import Path

from pytest import fixture

from src.smelters import Smelter


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

    return Path(tmpdir)


@fixture
def config_dict():
    return {
        "username": "Test_User",
        "api_key": "Test_API_Key",
        "hostname": "https://test.mytardis.nectar.auckland.ac.nz",
        "verify_certificate": True,
        "proxy_http": "http://myproxy.com",
        "proxy_https": "http://myproxy.com",
        "remote_directory": "/remote/path",
        "mount_directory": "/mount/path",
        "storage_box": "Test_storage_box",
        "default_institution": "Test Institution",
        "default_schema": {
            "project": "https://test.mytardis.nectar.auckland.ac.nz/project/v1",
            "experiment": "https://test.mytardis.nectar.auckland.ac.nz/experiment/v1",
            "dataset": "https://test.mytardis.nectar.auckland.ac.nz/dataset/v1",
            "datafile": "https://test.mytardis.nectar.auckland.ac.nz/datafile/v1",
        },
    }


@fixture
def introspection_response_dict():
    return {
        "meta": {
            "limit": 20,
            "next": None,
            "offset": 0,
            "previous": None,
            "total_count": 1,
        },
        "objects": [
            {
                "experiment_only_acls": False,
                "identified_objects": [
                    "dataset",
                    "experiment",
                    "facility",
                    "instrument",
                    "project",
                    "institution",
                ],
                "identifiers_enabled": True,
                "profiled_objects": [],
                "profiles_enabled": False,
                "projects_enabled": True,
                "resource_uri": "/api/v1/introspection/None/",
            }
        ],
    }


@fixture
def processed_introspection_response():
    return {
        "old_acls": False,
        "projects_enabled": True,
        "objects_with_ids": [
            "dataset",
            "experiment",
            "facility",
            "instrument",
            "project",
            "institution",
        ],
    }


@fixture
def institution_response_dict():
    return {
        "meta": {
            "limit": 20,
            "next": None,
            "offset": 0,
            "previous": None,
            "total_count": 1,
        },
        "objects": [
            {
                "address": "words",
                "aliases": 1,
                "alternate_ids": ["fruit", "apples"],
                "country": "NZ",
                "name": "University of Auckland",
                "persistent_id": "Uni ROR",
                "resource_uri": "/api/v1/institution/1/",
            }
        ],
    }


@fixture
def raw_project_dictionary():
    return {
        "name": "Test Project",
        "persistent_id": "Project_1",
        "alternate_ids": [
            "Test_Project",
            "Project_Test_1",
        ],
        "description": "A test project for the purposes of testing",
        "principal_investigator": "upi001",
        "admin_groups": ["Test_Group_1"],
        "admin_users": ["upi002", "upi003"],
        "metadata": {
            "My Test Key 1": "Test Value",
            "My Test Key 2": "Test Value 2",
        },
    }


@fixture
def tidied_project_dictionary():
    return {
        "name": "Test Project",
        "persistent_id": "Project_1",
        "alternate_ids": [
            "Test_Project",
            "Project_Test_1",
        ],
        "description": "A test project for the purposes of testing",
        "principal_investigator": "upi001",
        "admin_groups": ["Test_Group_1"],
        "admin_users": ["upi002", "upi003"],
        "project_my_test_key_1": "Test Value",
        "project_my_test_key_2": "Test Value 2",
        "schema": "https://test.mytardis.nectar.auckland.ac.nz/project/v1",
        "institution": "Test Institution",
    }


@fixture
def raw_experiment_dictionary():
    return {
        "title": "Test Experiment",
        "projects": [
            "Project_1",
            "Test_Project",
        ],
        "persistent_id": "Experiment_1",
        "alternate_ids": [
            "Test_Experiment",
            "Experiment_Test_1",
        ],
        "description": "A test experiment for the purposes of testing",
        "metadata": {
            "My Test Key 1": "Test Value",
            "My Test Key 2": "Test Value 2",
        },
    }


@fixture
def tidied_experiment_dictionary():
    return {
        "title": "Test Experiment",
        "projects": [
            "Project_1",
            "Test_Project",
        ],
        "persistent_id": "Experiment_1",
        "alternate_ids": [
            "Test_Experiment",
            "Experiment_Test_1",
        ],
        "description": "A test experiment for the purposes of testing",
        "experiment_my_test_key_1": "Test Value",
        "experiment_my_test_key_2": "Test Value 2",
        "schema": "https://test.mytardis.nectar.auckland.ac.nz/experiment/v1",
    }


@fixture
def raw_dataset_dictionary():
    return {
        "description": "Test Dataset",
        "experiments": [
            "Experiment_1",
            "Test_Experiment",
        ],
        "persistent_id": "Dataset_1",
        "alternate_ids": [
            "Test_Dataset",
            "Dataset_Test_1",
        ],
        "instrument": "Instrument_1",
        "metadata": {
            "My Test Key 1": "Test Value",
            "My Test Key 2": "Test Value 2",
        },
    }


@fixture
def tidied_dataset_dictionary():
    return {
        "description": "Test Dataset",
        "experiments": [
            "Experiment_1",
            "Test_Experiment",
        ],
        "persistent_id": "Dataset_1",
        "alternate_ids": [
            "Test_Dataset",
            "Dataset_Test_1",
        ],
        "instrument": "Instrument_1",
        "dataset_my_test_key_1": "Test Value",
        "dataset_my_test_key_2": "Test Value 2",
        "schema": "https://test.mytardis.nectar.auckland.ac.nz/dataset/v1",
    }


@fixture
def raw_datafile_dictionary():
    return {
        "dataset": ["Dataset_1"],
        "filename": "test_data.dat",
        "file_path": Path("/mount/remote/test_data.dat"),
        "md5sum": "0d32909e86e422d04a053d1ba26a990e",
        "full_path": "/mount/remote/test_data.dat",
        "metadata": {
            "My Test Key 1": "Test Value",
            "My Test Key 2": "Test Value 2",
        },
        "size": 52428800,
    }


@fixture
def tidied_datafile_dictionary():
    return {
        "dataset": ["Dataset_1"],
        "filename": "test_data.dat",
        "md5sum": "0d32909e86e422d04a053d1ba26a990e",
        "datafile_my_test_key_1": "Test Value",
        "datafile_my_test_key_2": "Test Value 2",
        "size": 52428800,
        "replicas": [
            {
                "uri": "test_data.dat",
                "location": "Test_storage_box",
                "protocol": "file",
            },
        ],
        "schema": "https://test.mytardis.nectar.auckland.ac.nz/datafile/v1",
    }


@fixture
def mytardis_config(config_dict):
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
def smelter(mytardis_config):
    Smelter.__abstractmethods__ = set()
    smelter = Smelter(mytardis_config)
    smelter.OBJECT_KEY_CONVERSION = {
        "project": {
            "project_name": "name",
            "lead_researcher": "principal_investigator",
            "project_id": "persistent_id",
        },
        "experiment": {
            "experiment_name": "title",
            "experiment_id": "persistent_id",
            "project_id": "projects",
        },
        "dataset": {
            "dataset_name": "description",
            "experiment_id": "experiments",
            "dataset_id": "persistent_id",
            "instrument_id": "instrument",
        },
        "datafile": {},
    }
    smelter.OBJECT_TYPES = {
        "project_name": "project",
        "experiment_name": "experiment",
        "dataset_name": "dataset",
        "datafiles": "datafile",
    }
    return smelter
