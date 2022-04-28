# NB: For the purposes of testing the IngestionFactory codebase a YAML specific
# implementation of the IngestionFactory is used. This concrete class defines
# the YAMLSmelter class to be the smelter implementation. As this class is fully
# tested, there is little risk in using this as a test case.

import logging
from pathlib import Path

import pytest
import responses
from responses import matchers

from src.specific_implementations import YAMLIngestionFactory

from .conftest import datadir

logger = logging.getLogger(__name__)
logger.propagate = True

config_dict = {
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

introspection_response_dict = {
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

institution_response_dict = {
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

search_with_no_uri_dict = {"institution": "Uni RoR"}


@responses.activate
def test_build_object_lists(datadir):
    responses.add(
        responses.GET,
        f"{config_dict['hostname']}/api/v1/introspection",
        status=200,
        json=(introspection_response_dict),
    )
    factory = YAMLIngestionFactory(config_dict)
    test_projects = [
        Path(datadir / "test_mixed.yaml"),
        Path(datadir / "test_project.yaml"),
    ]
    test_experiments = [
        Path(datadir / "test_experiment.yaml"),
        Path(datadir / "test_mixed.yaml"),
    ]
    test_datasets = [
        Path(datadir / "test_dataset.yaml"),
    ]
    test_datafiles = [
        Path(datadir / "test_datafile.yaml"),
    ]
    assert factory.build_object_lists(datadir, "project") == test_projects
    assert factory.build_object_lists(datadir, "experiment") == test_experiments
    assert factory.build_object_lists(datadir, "dataset") == test_datasets
    assert factory.build_object_lists(datadir, "datafile") == test_datafiles


@responses.activate
def test_build_object_lists_rename_this(datadir):
    object_type = "institution"
    pid = "Uni RoR"
    responses.add(
        responses.GET,
        f"{config_dict['hostname']}/api/v1/introspection",
        status=200,
        json=(introspection_response_dict),
    )
    responses.add(
        responses.GET,
        f"{config_dict['hostname']}/api/v1/institution",
        match=[matchers.query_param_matcher({"pids": pid})],
        status=200,
        json=(institution_response_dict),
    )
    factory = YAMLIngestionFactory(config_dict)
    assert factory.replace_search_term_with_uri(
        object_type, search_with_no_uri_dict, "name"
    ) == {"institution": ["/api/v1/institution/1/"]}
