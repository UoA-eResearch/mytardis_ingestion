from pytest import fixture


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
