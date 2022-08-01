# pylint: disable=missing-function-docstring

import shutil
from pathlib import Path
from typing import List

from pytest import fixture

import tests.fixtures_constants as const
import tests.fixtures_dataclasses as dcls
from src.crucible import Crucible
from src.forges.forge import Forge
from src.helpers.config import (
    AuthConfig,
    ConfigFromEnv,
    ConnectionConfig,
    GeneralConfig,
    IntrospectionConfig,
    ProxyConfig,
    SchemaConfig,
    StorageConfig,
)
from src.helpers.mt_rest import MyTardisRESTFactory
from src.ingestion_factory import IngestionFactory
from src.overseers.inspector import Inspector
from src.overseers.overseer import Overseer
from src.smelters import Smelter

# =============================
#
# Base fixtures on which all others are built
#
# ==============================

username = const.username
api_key = const.api_key
hostname = const.hostname
verify_certificate = const.verify_certificate
proxies = const.proxies
source_dir = const.source_dir
target_dir = const.target_dir
default_institution = const.default_institution
old_acls = const.old_acls
projects_enabled = const.projects_enabled
objects_with_ids = const.objects_with_ids
filename = const.filename
admin_groups = const.admin_groups
read_groups = const.read_groups
download_groups = const.download_groups
sensitive_groups = const.sensitive_groups
admin_users = const.admin_users
read_users = const.read_users
download_users = const.download_users
sensitive_users = const.sensitive_users
project_name = const.project_name
project_description = const.project_description
project_pid = const.project_pid
project_ids = const.project_ids
project_principal_investigator = const.project_principal_investigator
project_institutions = const.project_institutions
project_metadata = const.project_metadata
project_schema = const.project_schema
project_metadata_processed = const.project_metadata_processed
project_url = const.project_url
experiment_name = const.experiment_name
experiment_description = const.experiment_description
experiment_institution = const.experiment_institution
experiment_projects = const.experiment_projects
experiment_pid = const.experiment_pid
experiment_ids = const.experiment_ids
experiment_metadata = const.experiment_metadata
experiment_schema = const.experiment_schema
experiment_metadata_processed = const.experiment_metadata_processed
dataset_dir = const.dataset_dir
dataset_name = const.dataset_name
dataset_experiments = const.dataset_experiments
dataset_instrument = const.dataset_instrument
datset_pid = const.dataset_pid
dataset_ids = const.dataset_ids
dataset_metadata = const.dataset_metadata
dataset_schema = const.dataset_schema
dataset_metadata_processed = const.dataset_metadata_processed
datafile_md5sum = const.datafile_md5sum
datafile_mimetype = const.datafile_mimetype
datafile_size = const.datafile_size
datafile_dataset = const.datafile_dataset
datafile_metadata = const.datafile_metadata
datafile_schema = const.datafile_schema
datafile_metadata_processed = const.datafile_metadata_processed
split_and_parse_groups = const.split_and_parse_groups
split_and_parse_users = const.split_and_parse_users
storage_box_name = const.storage_box_name
storage_box_uri = const.storage_box_uri
storage_box_description = const.storage_box_description
storage_box_dir = const.storage_box_dir
start_time_datetime = const.start_time_datetime
start_time_str = const.start_time_str
end_time_datetime = const.end_time_datetime
end_time_str = const.end_time_str
modified_time_datetime = const.modified_time_datetime
modified_time_str = const.modified_time_str
embargo_time_datetime = const.embargo_time_datetime
embargo_time_str = const.embargo_time_str
created_by_upi = const.created_by_upi
created_time_datetime = const.created_time_datetime
created_time_str = const.created_time_str

# =============================
#
# Helper functions
#
# ==============================


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
            if not source.is_dir():
                shutil.copy(source, tmpdir)

    return Path(tmpdir)


# =============================
#
# Dataclasses
#
# ==============================

storage_box = dcls.storage_box
datafile_replica = dcls.datafile_replica
raw_project_parameterset = dcls.raw_project_parameterset
raw_project = dcls.raw_project
raw_experiment_parameterset = dcls.raw_experiment_parameterset
raw_experiment = dcls.raw_experiment
raw_dataset_parameterset = dcls.raw_dataset_parameterset
raw_dataset = dcls.raw_dataset
raw_datafile_parameterset = dcls.raw_datafile_parameterset
raw_datafile = dcls.raw_datafile
refined_project = dcls.refined_project
refined_experiment = dcls.refined_experiment
refined_datafile = dcls.refined_datafile
refined_dataset = dcls.refined_dataset
# =============================
#
# Helper functions
#
# ==============================
# =============================
#
# Helper functions
#
# ==============================
# =============================
#
# Helper functions
#
# ==============================


# =========================================
#
# Dictionary fixtures
#
# =========================================


@fixture
def raw_project_dictionary(
    admin_groups,
    admin_users,
    download_groups,
    download_users,
    project_description,
    project_ids,
    project_metadata,
    project_name,
    project_pid,
    project_principal_investigator,
    read_groups,
    read_users,
    sensitive_groups,
    sensitive_users,
):
    return {
        "name": project_name,
        "persistent_id": project_pid,
        "alternate_ids": project_ids,
        "description": project_description,
        "principal_investigator": project_principal_investigator,
        "admin_groups": admin_groups,
        "admin_users": admin_users,
        "read_groups": read_groups,
        "read_users": read_users,
        "download_groups": download_groups,
        "download_users": download_users,
        "sensitive_groups": sensitive_groups,
        "sensitive_users": sensitive_users,
        "metadata": project_metadata,
    }


@fixture
def tidied_project_dictionary(
    admin_groups,
    admin_users,
    download_groups,
    download_users,
    project_description,
    project_ids,
    project_metadata,
    project_name,
    project_pid,
    project_principal_investigator,
    read_groups,
    read_users,
    sensitive_groups,
    sensitive_users,
    project_institutions,
    project_schema,
    split_and_parse_users,
    split_and_parse_groups,
):
    return_dict = {
        "name": project_name,
        "persistent_id": project_pid,
        "alternate_ids": project_ids,
        "description": project_description,
        "principal_investigator": project_principal_investigator,
        "users": split_and_parse_users,
        "groups": split_and_parse_groups,
        "schema": project_schema,
    }
    for key in project_metadata.keys():
        return_dict[key] = project_metadata[key]
    return return_dict


@fixture
def raw_project_as_dict(
    project_name,
    project_description,
    project_ids,
    project_pid,
    project_principal_investigator,
    split_and_parse_users,
    split_and_parse_groups,
):
    return {
        "name": project_name,
        "description": project_description,
        "principal_investigator": project_principal_investigator,
        "users": split_and_parse_users,
        "groups": split_and_parse_groups,
        "persistent_id": project_pid,
        "alternate_ids": project_ids,
    }


@fixture
def project_parameters_as_dict(
    project_schema,
    project_metadata,
):
    return_dict = {"schema": project_schema}
    return_dict["parameters"] = []
    for key, value in project_metadata.items():
        return_dict["parameters"].append({"name": key, "value": value})
    return return_dict


@fixture
def raw_experiment_dictionary(
    experiment_name,
    experiment_projects,
    experiment_pid,
    experiment_ids,
    experiment_description,
    experiment_metadata,
):
    return {
        "title": experiment_name,
        "projects": experiment_projects,
        "persistent_id": experiment_pid,
        "alternate_ids": experiment_ids,
        "description": experiment_description,
        "metadata": experiment_metadata,
    }


@fixture
def tidied_experiment_dictionary(
    experiment_name,
    experiment_projects,
    experiment_pid,
    experiment_ids,
    experiment_description,
    experiment_metadata,
    experiment_schema,
):
    return_dict = {
        "title": experiment_name,
        "projects": experiment_projects,
        "persistent_id": experiment_pid,
        "alternate_ids": experiment_ids,
        "description": experiment_description,
        "schema": experiment_schema,
    }
    for key in experiment_metadata.keys():
        return_dict[key] = experiment_metadata[key]
    return return_dict


@fixture
def raw_experiment_as_dict(
    experiment_name,
    experiment_projects,
    experiment_description,
    experiment_pid,
    experiment_ids,
):
    return {
        "title": experiment_name,
        "projects": experiment_projects,
        "description": experiment_description,
        "persistent_id": experiment_pid,
        "alternate_ids": experiment_ids,
    }


@fixture
def experiment_parameters_as_dict(
    experiment_schema,
    experiment_metadata,
):
    return_dict = {"schema": experiment_schema}
    return_dict["parameters"] = []
    for key, value in experiment_metadata.items():
        return_dict["parameters"].append({"name": key, "value": value})
    return return_dict


@fixture
def raw_dataset_dictionary(
    dataset_name,
    dataset_experiments,
    dataset_pid,
    dataset_ids,
    dataset_instrument,
    dataset_metadata,
):
    return {
        "description": dataset_name,
        "experiments": dataset_experiments,
        "persistent_id": dataset_pid,
        "alternate_ids": dataset_ids,
        "instrument": dataset_instrument,
        "metadata": dataset_metadata,
    }


@fixture
def tidied_dataset_dictionary(
    dataset_name,
    dataset_experiments,
    dataset_pid,
    dataset_ids,
    dataset_instrument,
    dataset_schema,
    dataset_metadata,
):
    return_dict = {
        "description": dataset_name,
        "experiments": dataset_experiments,
        "persistent_id": dataset_pid,
        "alternate_ids": dataset_ids,
        "instrument": dataset_instrument,
        "schema": dataset_schema,
    }
    for key in dataset_metadata.keys():
        return_dict[key] = dataset_metadata[key]
    return return_dict


@fixture
def raw_dataset_as_dict(
    dataset_name,
    dataset_experiments,
    dataset_pid,
    dataset_ids,
    dataset_instrument,
):
    return {
        "description": dataset_name,
        "experiments": dataset_experiments,
        "persistent_id": dataset_pid,
        "alternate_ids": dataset_ids,
        "instrument": dataset_instrument,
    }


@fixture
def dataset_parameters_as_dict(
    dataset_schema,
    dataset_metadata,
):
    return_dict = {"schema": dataset_schema}
    return_dict["parameters"] = []
    for key, value in dataset_metadata.items():
        return_dict["parameters"].append({"name": key, "value": value})
    return return_dict


@fixture
def raw_datafile_dictionary(
    datafile_dataset,
    filename,
    target_dir,
    source_dir,
    datafile_md5sum,
    datafile_metadata,
    datafile_size,
):
    return {
        "dataset": datafile_dataset,
        "filename": filename,
        "relative_file_path": Path(target_dir) / Path(filename),
        "md5sum": datafile_md5sum,
        "full_path": Path(source_dir) / Path(filename),
        "metadata": datafile_metadata,
        "size": datafile_size,
    }


@fixture
def tidied_datafile_dictionary(
    datafile_dataset,
    filename,
    target_dir,
    source_dir,
    datafile_md5sum,
    datafile_metadata,
    datafile_size,
    directory_relative_to_storage_box,
    datafile_replica,
    datafile_schema,
    datafile_mimetype,
):
    return_dict = {
        "dataset": datafile_dataset,
        "filename": filename,
        "relative_file_path": Path(target_dir) / Path(filename),
        "md5sum": datafile_md5sum,
        "mimetype": datafile_mimetype,
        "full_path": Path(source_dir) / Path(filename),
        "size": datafile_size,
        "directory": directory_relative_to_storage_box,
        "replicas": [datafile_replica],
        "schema": datafile_schema,
    }
    for key in datafile_metadata.keys():
        return_dict[key] = datafile_metadata[key]
    return return_dict


@fixture
def raw_datafile_as_dict(
    datafile_dataset,
    filename,
    target_dir,
    source_dir,
    datafile_md5sum,
    datafile_size,
    directory_relative_to_storage_box,
    datafile_replica,
    datafile_mimetype,
):
    return {
        "dataset": datafile_dataset,
        "filename": filename,
        "relative_file_path": Path(target_dir) / Path(filename),
        "md5sum": datafile_md5sum,
        "full_path": Path(source_dir) / Path(filename),
        "size": datafile_size,
        "directory": directory_relative_to_storage_box,
        "replicas": [datafile_replica],
        "mimetype": datafile_mimetype,
    }


@fixture
def datafile_parameters_as_dict(
    datafile_schema,
    datafile_metadata,
):
    return_dict = {"schema": datafile_schema}
    return_dict["parameters"] = []
    for key, value in datafile_metadata.items():
        return_dict["parameters"].append({"name": key, "value": value})
    return return_dict


@fixture
def preconditioned_datafile_dictionary():
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
        "file_path": "test_data.dat",
    }


# =========================================
#
# Dataclass fixtures
#
# =========================================


# =========================================
#
# Config fixtures
#
# =========================================


@fixture
def config_dict(
    username,
    api_key,
    hostname,
    verify_certificate,
    proxies,
    source_dir,
    target_dir,
    storage_box,
    default_institution,
    project_schema,
    experiment_schema,
    dataset_schema,
    datafile_schema,
):
    return {
        "username": username,
        "api_key": api_key,
        "hostname": hostname,
        "verify_certificate": verify_certificate,
        "proxy_http": proxies["http"],
        "proxy_https": proxies["https"],
        "source_directory": source_dir,
        "target_directory": target_dir,
        "storage_box": storage_box.name,
        "default_institution": default_institution,
        "default_schema": {
            "project": project_schema,
            "experiment": experiment_schema,
            "dataset": dataset_schema,
            "datafile": datafile_schema,
        },
    }


@fixture
def processed_introspection_response(old_acls, projects_enabled, objects_with_ids):
    return {
        "old_acls": old_acls,
        "projects_enabled": projects_enabled,
        "objects_with_ids": objects_with_ids,
    }


@fixture
def mytardis_config(config_dict, projects_enabled, objects_with_ids):
    configuration = config_dict
    configuration["projects_enabled"] = projects_enabled
    configuration["objects_with_ids"] = objects_with_ids
    return configuration


# =========================================
#
# Mocked responses
#
# =========================================


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
def instrument_response_dict():
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
def storage_box_response_dict(
    storage_box_description,
    storage_box_name,
    storage_box_dir,
    storage_box_uri,
):
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
                "description": storage_box_description,
                "django_storage_class": "django.core.files.storage.FileSystemStorage",
                "id": 1,
                "max_size": 1000000,
                "name": storage_box_name,
                "options": [
                    {
                        "id": 1,
                        "key": "location",
                        "resource_uri": "/api/v1/storageboxoption/1/",
                        "value": storage_box_dir,
                        "value_type": "string",
                    },
                ],
                "resource_uri": storage_box_uri,
                "status": "online",
            },
        ],
    }


@fixture
def project_creation_response_dict():
    return {
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

    # =========================================
    #
    # config from env classes
    #
    # =========================================


@fixture
def general(default_institution) -> GeneralConfig:
    return GeneralConfig(default_institution=default_institution)


@fixture
def auth(username, api_key) -> AuthConfig:
    return AuthConfig(username=username, api_key=api_key)


@fixture
def connection(hostname, proxies, verify_certificate) -> ConnectionConfig:
    return ConnectionConfig(
        hostname=hostname,
        proxy=ProxyConfig(http=proxies["http"], https=proxies["https"]),
        verify_certificate=verify_certificate,
    )


@fixture
def storage(storage_box, source_dir, target_dir) -> StorageConfig:
    return StorageConfig(
        box=storage_box.name,
        source_directory=source_dir,
        target_directory=target_dir,
    )


@fixture
def default_schema(
    project_schema, experiment_schema, dataset_schema, datafile_schema
) -> SchemaConfig:
    return SchemaConfig(
        project=project_schema,
        experiment=experiment_schema,
        dataset=dataset_schema,
        datafile=datafile_schema,
    )


@fixture
def mytardis_setup(processed_introspection_response) -> IntrospectionConfig:
    return IntrospectionConfig(
        old_acls=processed_introspection_response["old_acls"],
        projects_enabled=processed_introspection_response["projects_enabled"],
        objects_with_ids=processed_introspection_response["objects_with_ids"],
    )


@fixture
def mytardis_settings_no_introspection(
    general: GeneralConfig,
    auth: AuthConfig,
    connection: ConnectionConfig,
    storage: StorageConfig,
    default_schema: SchemaConfig,
) -> ConfigFromEnv:
    return ConfigFromEnv(
        general=general,
        auth=auth,
        connection=connection,
        storage=storage,
        default_schema=default_schema,
    )


@fixture
def mytardis_settings(
    mytardis_settings_no_introspection: ConfigFromEnv,
    mytardis_setup: IntrospectionConfig,
) -> ConfigFromEnv:
    mytardis_settings_no_introspection._mytardis_setup = mytardis_setup
    return mytardis_settings_no_introspection


# =========================================
#
# Ingestion classes
#
# =========================================


@fixture
def rest_factory(auth: AuthConfig, connection: ConnectionConfig):
    return MyTardisRESTFactory(auth, connection)


@fixture
def overseer(rest_factory: MyTardisRESTFactory, mytardis_setup: IntrospectionConfig):
    return Overseer(rest_factory, mytardis_setup)


@fixture
def smelter(
    general: GeneralConfig,
    default_schema: SchemaConfig,
    storage: StorageConfig,
    storage_box: StorageBox,
    mytardis_setup: IntrospectionConfig,
):
    return Smelter(general, default_schema, storage, storage_box, mytardis_setup)


@fixture
def forge(rest_factory: MyTardisRESTFactory):
    return Forge(rest_factory)


@fixture
def crucible(overseer: Overseer, mytardis_setup: IntrospectionConfig):
    return Crucible(overseer, mytardis_setup)


@fixture
def inspector(overseer: Overseer, mytardis_setup: IntrospectionConfig):
    return Inspector(overseer, mytardis_setup)


@fixture
def factory(
    smelter: Smelter,
    inspector: Inspector,
    forge: Forge,
    overseer: Overseer,
    crucible: Crucible,
):
    return IngestionFactory(smelter, inspector, forge, overseer, crucible)
