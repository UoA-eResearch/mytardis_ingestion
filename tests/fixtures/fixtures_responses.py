from pathlib import Path
from pytest import fixture
from src.blueprints.custom_data_types import URI
from src.blueprints.project import Project

from src.helpers.enumerators import URLSubstring


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
def datafile_response_dict(
    filename: str,
    datafile_md5sum: str,
    datafile_mimetype: str,
    datafile_size: int,
    datafile_dataset: str,
):
    return {
        "meta": {
            "limit": 20,
            "next": None,
            "offset": 0,
            "previous": None,
            "total_count": 6,
        },
        "objects": [
            {
                "created_time": None,
                "datafile": None,
                "dataset": datafile_dataset,
                "deleted": False,
                "deleted_time": None,
                "directory": None,
                "filename": filename,
                "id": 1,
                "md5sum": datafile_md5sum,
                "mimetype": datafile_mimetype,
                "modification_time": None,
                "parameter_sets": [],
                "public_access": 100,
                "replicas": [],
                "resource_uri": "/api/v1/dataset_file/1002735/",
                "sha512sum": "",
                "size": datafile_size,
                "tags": [],
                "version": 1,
            }
        ],
    }


@fixture
def dataset_response_dict(
    dataset_dir: Path,
    dataset_name: str,
    dataset_description: str,
    dataset_experiments: list[str],
    dataset_instrument: str,
    dataset_pid: str,
    dataset_ids: list[str],
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
                "alternate_ids": dataset_ids,
                "created_time": "2000-01-01T00:00:00",
                "dataset_datafile_count": 2,
                "dataset_experiment_count": 1,
                "dataset_size": 1000000,
                "description": dataset_name,  # TODO should this be name or description, no dataset name???
                "directory": dataset_dir.as_posix(),
                "experiments": dataset_experiments,
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
                    "name": dataset_instrument,
                    "persistent_id": "Instrument_1",
                    "resource_uri": "/api/v1/instrument/1/",
                },
                "modified_time": "2000-01-01T00:00:00",
                "parameter_sets": [],
                "persistent_id": dataset_pid,
                "public_access": 1,
                "resource_uri": "/api/v1/dataset/1/",
                "tags": [],
            }
        ],
    }


@fixture
def experiment_response_dict(
    experiment_pid: str,
    experiment_ids: list[str],
    experiment_description: str,
    experiment_institution: str,
    experiment_name: str,
    experiment_url: str,
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
                "alternate_ids": experiment_ids,
                "approved": False,
                "authors": [],
                "created_by": "api/v1/user/1/",
                "created_time": "2000-01-01T00:00:00",
                "datafile_count": 2,
                "dataset_count": 1,
                "description": experiment_description,
                "end_time": None,
                "experiment_size": 1000000,
                "handle": None,
                "id": 1,
                "institution_name": experiment_institution,
                "locked": False,
                "owner_ids": [
                    1,
                    2,
                ],
                "parameter_sets": [],
                "persistent_id": experiment_pid,
                "public_access": 1,
                "resource_uri": "/api/v1/experiment/1/",
                "start_time": "2000-01-01T00:00:00",
                "tags": [],
                "title": experiment_name,
                "update_time": "2000-01-01T00:00:00",
                "url": experiment_url,
            }
        ],
    }


@fixture
def project_response_dict(
    project_pid: str,
    project_ids: list[str],
    project_description: str,
    project_institutions: list[str],
    project_name: str,
    project_principal_investigator: str,
    project_url: str,
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
                "alternate_ids": project_ids,
                "created_by": "api/v1/user/1/",
                "datafile_count": 2,
                "dataset_count": 1,
                "description": project_description,
                "embargo_until": None,
                "end_time": None,
                "experiment_count": 1,
                "id": 1,
                "institution": project_institutions,
                "locked": False,
                "name": project_name,
                "parameter_sets": [],
                "persistent_id": project_pid,
                "principal_investigator": project_principal_investigator,
                "public_access": 1,
                "resource_uri": "/api/v1/project/1/",
                "size": 1000000,
                "start_time": "2000-01-01T00:00:00",
                "tags": [],
                "url": project_url,
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
    storage_box_dir: Path,
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
                        "value": storage_box_dir.as_posix(),
                        "value_type": "string",
                    },
                ],
                "resource_uri": storage_box_uri,
                "status": "online",
            },
        ],
    }


@fixture
def project_creation_response_dict(
    project: Project, project_uri: URI, institution_uri: URI, user_uri: URI
):
    return {
        "alternate_ids": project.alternate_ids,
        "created_by": user_uri,
        "datafile_count": 2,
        "dataset_count": 1,
        "description": project.description,
        "embargo_until": project.embargo_until,
        "end_time": project.end_time,
        "experiment_count": 1,
        "id": 1,
        "institution": [
            institution_uri,
        ],
        "locked": False,
        "name": project.name,
        "parameter_sets": [],
        "persistent_id": project.persistent_id,
        "principal_investigator": project.persistent_id,
        "public_access": 1,
        "resource_uri": project_uri,
        "size": 1000000,
        "start_time": project.start_time,
        "tags": [],
        "url": project.url,
    }


@fixture
def response_by_substring(
    response_dict_not_found,
    dataset_response_dict,
    experiment_response_dict,
    project_response_dict,
    datafile_response_dict,
):
    def _get_response_dict(substring: URLSubstring):
        match substring:
            case URLSubstring.PROJECT:
                return project_response_dict
            case URLSubstring.EXPERIMENT:
                return experiment_response_dict
            case URLSubstring.DATASET:
                return dataset_response_dict
            case URLSubstring.DATAFILE:
                return datafile_response_dict
        return response_dict_not_found

    return _get_response_dict
