# pylint: disable=missing-function-docstring,redefined-outer-name,missing-module-docstring

import shutil
from pathlib import Path

from pytest import fixture

import tests.fixtures.fixtures_config_from_env as cfg
import tests.fixtures.fixtures_constants as const
import tests.fixtures.fixtures_dataclasses as dcls
import tests.fixtures.fixtures_ingestion_classes as ingestion_classes
import tests.fixtures.fixtures_responses as rsps
import tests.fixtures.mock_rest_factory as mock_rest

# =============================
#
# Base fixtures on which all others are built
#
# ==============================

timezone = const.timezone
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
experiment_ids = const.experiment_ids
experiment_metadata = const.experiment_metadata
experiment_schema = const.experiment_schema
experiment_url = const.experiment_url
experiment_metadata_processed = const.experiment_metadata_processed
dataset_dir = const.dataset_dir
dataset_name = const.dataset_name
dataset_description = const.dataset_description
dataset_experiments = const.dataset_experiments
dataset_instrument = const.dataset_instrument
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
instrument_ids = const.instrument_ids
instrument_name = const.instrument_name
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
directory_relative_to_storage_box = const.directory_relative_to_storage_box
project_uri = const.project_uri
experiment_uri = const.experiment_uri
dataset_uri = const.dataset_uri
datafile_uri = const.datafile_uri
institution_uri = const.institution_uri
instrument_uri = const.instrument_uri
user_uri = const.user_uri
institution_address = const.institution_address
institution_ids = const.institution_ids
institution_country = const.institution_country
institution_name = const.institution_name
project_data_classification = const.project_data_classification
experiment_data_classification = const.experiment_data_classification
dataset_data_classification = const.dataset_data_classification


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
project = dcls.project
experiment = dcls.experiment
dataset = dcls.dataset
datafile = dcls.datafile


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
    project_principal_investigator,
    read_groups,
    read_users,
    sensitive_groups,
    sensitive_users,
):
    return {
        "name": project_name,
        "identifers": project_ids,
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
    project_description,
    project_ids,
    project_metadata,
    project_name,
    project_principal_investigator,
    project_institutions,
    project_schema,
    split_and_parse_users,
    split_and_parse_groups,
):
    return_dict = {
        "name": project_name,
        "identifers": project_ids,
        "description": project_description,
        "principal_investigator": project_principal_investigator,
        "users": split_and_parse_users,
        "groups": split_and_parse_groups,
        "schema": project_schema,
        "institution": project_institutions,
    }
    for key in project_metadata.keys():
        return_dict[key] = project_metadata[key]
    return return_dict


@fixture
def raw_project_as_dict(
    project_name,
    project_description,
    project_ids,
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
        "identifers": project_ids,
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
    experiment_ids,
    experiment_description,
    experiment_metadata,
):
    return {
        "title": experiment_name,
        "projects": experiment_projects,
        "identifiers": experiment_ids,
        "description": experiment_description,
        "metadata": experiment_metadata,
    }


@fixture
def tidied_experiment_dictionary(
    experiment_name,
    experiment_projects,
    experiment_ids,
    experiment_description,
    experiment_metadata,
    experiment_schema,
):
    return_dict = {
        "title": experiment_name,
        "projects": experiment_projects,
        "identifiers": experiment_ids,
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
    experiment_ids,
):
    return {
        "title": experiment_name,
        "projects": experiment_projects,
        "description": experiment_description,
        "identifiers": experiment_ids,
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
    dataset_ids,
    dataset_instrument,
    dataset_metadata,
):
    return {
        "description": dataset_name,
        "experiments": dataset_experiments,
        "identifiers": dataset_ids,
        "instrument": dataset_instrument,
        "metadata": dataset_metadata,
    }


@fixture
def tidied_dataset_dictionary(
    dataset_name,
    dataset_experiments,
    dataset_ids,
    dataset_instrument,
    dataset_schema,
    dataset_metadata,
):
    return_dict = {
        "description": dataset_name,
        "experiments": dataset_experiments,
        "identifiers": dataset_ids,
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
    dataset_ids,
    dataset_instrument,
):
    return {
        "description": dataset_name,
        "experiments": dataset_experiments,
        "identifiers": dataset_ids,
        "instrument": dataset_instrument,
    }


#    for key in dataset_metadata.keys():
#        return_dict[key] = dataset_metadata[key]
#    return return_dict


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
# Mocked responses
#
# =========================================

response_dict_not_found = rsps.response_dict_not_found
datafile_response_dict = rsps.datafile_response_dict
dataset_response_dict = rsps.dataset_response_dict
experiment_response_dict = rsps.experiment_response_dict
project_response_dict = rsps.project_response_dict
instrument_response_dict = rsps.instrument_response_dict
introspection_response_dict = rsps.introspection_response_dict
institution_response_dict = rsps.institution_response_dict
storage_box_response_dict = rsps.storage_box_response_dict
project_creation_response_dict = rsps.project_creation_response_dict
response_by_substring = rsps.response_by_substring


# =========================================
#
# config from env classes
#
# =========================================

processed_introspection_response = cfg.processed_introspection_response
general = cfg.general
auth = cfg.auth
connection = cfg.connection
storage = cfg.storage
default_schema = cfg.default_schema
mytardis_setup = cfg.mytardis_setup
mytardis_settings = cfg.mytardis_settings


# =========================================
#
# Ingestion classes
#
# =========================================


rest_factory = ingestion_classes.rest_factory
mock_mt_rest = mock_rest.mock_mt_rest
overseer = ingestion_classes.overseer
mock_overseer = ingestion_classes.mock_overseer
smelter = ingestion_classes.smelter
forge = ingestion_classes.forge
crucible = ingestion_classes.crucible
factory = ingestion_classes.factory
