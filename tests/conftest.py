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


############################
#
# Expected returns
#
############################


@fixture
def response_dict_not_found():
    return {
        "meta": {
            "limit": 20,
            "next": None,
            "offset": 0,
            "previous": None,
            "total_count": 0,
        },
        "objects": [],
    }


@fixture
def dataset_response_dict():
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
                "alternate_ids": [
                    "Test_Dataset",
                    "Dataset_Test_1",
                ],
                "created_time": "2000-01-01T00:00:00",
                "dataset_datafile_count": 2,
                "dataset_experiment_count": 1,
                "dataset_size": 1000000,
                "description": "A test dataset for the purposes of testing",
                "directory": None,
                "experiments": [
                    "/api/v1/experiment/1",
                ],
                "id": 1,
                "immutable": False,
                "instrument": {
                    "alternate_ids": [
                        "Test_Instrument",
                        "Instrument_Test_1",
                    ],
                    "created_time": "2000-01-01T00:00:00",
                    "facility": {
                        "alternate_ids": [
                            "Test_Facility",
                            "Facility_Test_1",
                        ],
                        "created_time": "2000-01-01T00:00:00",
                        "id": 1,
                        "manager_group": {
                            "name": "Test_Facility_Management_Group",
                            "id": 1,
                            "resource_uri": "/api/v1/group/1/",
                        },
                        "modified_time": "2000-01-01T00:00:00",
                        "name": "Test Facility",
                        "persistent_id": "Facility_1",
                        "resource_uri": "/api/v1/facility/1/",
                    },
                    "modified_time": "2000-01-01T00:00:00",
                    "name": "Test Instrument",
                    "persistent_id": "Instrument_1",
                    "resource_uri": "/api/v1/instrument/1/",
                },
                "modified_time": "2000-01-01T00:00:00",
                "parameter_sets": [],
                "persistent_id": "Dataset_1",
                "public_access": 1,
                "resource_uri": "/api/v1/dataset/1/",
                "tags": [],
            }
        ],
    }


@fixture
def experiment_response_dict():
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
                "alternate_ids": [
                    "Test_Experiment",
                    "Experiment_Test_1",
                ],
                "approved": False,
                "authors": [],
                "created_by": "api/v1/user/1/",
                "created_time": "2000-01-01T00:00:00",
                "datafile_count": 2,
                "dataset_count": 1,
                "description": "A test experiment for the purposes of testing",
                "end_time": None,
                "experiment_size": 1000000,
                "handle": None,
                "id": 1,
                "institution_name": "Test Institution",
                "locked": False,
                "owner_ids": [
                    1,
                    2,
                ],
                "parameter_sets": [],
                "persistent_id": "Experiment_1",
                "public_access": 1,
                "resource_uri": "/api/v1/experiment/1/",
                "start_time": "2000-01-01T00:00:00",
                "tags": [],
                "title": "Test Experiment",
                "update_time": "2000-01-01T00:00:00",
                "url": None,
            }
        ],
    }


@fixture
def project_response_dict():
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
                "alternate_ids": [
                    "Test_Project",
                    "Project_Test_1",
                ],
                "created_by": "api/v1/user/1/",
                "datafile_count": 2,
                "dataset_count": 1,
                "description": "A test project for the purposes of testing",
                "embargo_until": None,
                "end_time": None,
                "experiment_count": 1,
                "id": 1,
                "institution": [
                    "api/v1/institution/1/",
                ],
                "locked": False,
                "name": "Test Project",
                "parameter_sets": [],
                "persistent_id": "Project_1",
                "principal_investigator": "upi001",
                "public_access": 1,
                "resource_uri": "/api/v1/project/1/",
                "size": 1000000,
                "start_time": "2000-01-01T00:00:00",
                "tags": [],
                "url": None,
            }
        ],
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
def storage_box_response_dict():
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
                "attributes": [],
                "description": "Test Storage Box for testing",
                "django_storage_class": "django.core.files.storage.FileSystemStorage",
                "id": 1,
                "max_size": 1000000,
                "name": "Test_storage_box",
                "options": {
                    "id": 1,
                    "key": "location",
                    "resource_uri": "/api/v1/storageboxoption/1/",
                    "value": "/remote/path",
                    "value_type": "string",
                },
                "resource_uri": "/api/v1/storagebox/1/",
                "status": "online",
            },
        ],
    }
