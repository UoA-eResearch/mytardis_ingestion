# pylint: disable=missing-function-docstring

"""Tests of the Overseer class and its functions"""
import logging

import mock
import pytest

from src.overseers import Overseer

logger = logging.getLogger(__name__)
logger.propagate = True

config_dict = {
    "username": "Test User",
    "api_key": "Test API Key",
    "hostname": "https://test.mytardis.nectar.auckland.ac.nz",
    "verify_certificate": True,
}


def test_staticmethod_resource_uri_to_id():
    test_uri = "/api/v1/user/10/"
    assert Overseer.resource_uri_to_id(test_uri) == 10


@mock.patch("src.helpers.MyTardisRESTFactory.mytardis_api_request")
def test_overseer_setups_good(mock_rest_factory_mytardis_api_request):
    mock_response = mock.Mock()
    mock_response.json.return_value = (
        "{"
        '    "meta": {'
        '        "limit": 20,'
        '        "next": null,'
        '        "offset": 0,'
        '        "previous": null,'
        '        "total_count": 1'
        "    },"
        '    "objects": ['
        "        {"
        '            "experiment_only_acls": false,'
        '            "identified_objects": ['
        '                "project",'
        '                "experiment",'
        '                "dataset",'
        '                "instrument",'
        '                "facility"'
        "            ],"
        '            "identifiers_enabled": false,'
        '            "profiled_objects": [],'
        '            "profiles_enabled": false,'
        '            "projects_enabled": true,'
        '            "resource_uri": "/api/v1/introspection/None/"'
        "        }"
        "    ]"
        "}"
    )
    mock_rest_factory_mytardis_api_request.return_value = mock_response
    test_overseer = Overseer(config_dict)
    assert not test_overseer.old_acls
    assert test_overseer.projects_enabled
    assert test_overseer.objects_with_ids is None
    assert test_overseer.objects_with_profiles is None


@mock.patch("src.helpers.MyTardisRESTFactory.mytardis_api_request")
def test_overseer_setups_no_objects_returned(mock_rest_factory_mytardis_api_request):
    mock_response = mock.Mock()
    mock_response.json.return_value = (
        "{"
        '    "meta": {'
        '        "limit": 20,'
        '        "next": null,'
        '        "offset": 0,'
        '        "previous": null,'
        '        "total_count": 1'
        "    },"
        '    "objects": []'
        "}"
    )
    mock_rest_factory_mytardis_api_request.return_value = mock_response
    with pytest.raises(ValueError) as error:
        test_overseer = Overseer(config_dict)  # pylint: disable=unused-variable

    assert str(error.value) == (
        "MyTardis introspection did not return any data when called from "
        "Overseer.get_mytardis_set_up"
    )


@mock.patch("src.helpers.MyTardisRESTFactory.mytardis_api_request")
def test_overseer_setups_multiple_objects_returned(
    mock_rest_factory_mytardis_api_request, caplog
):
    mock_response = mock.Mock()
    mock_response.json.return_value = (
        "{"
        '    "meta": {'
        '        "limit": 20,'
        '        "next": null,'
        '        "offset": 0,'
        '        "previous": null,'
        '        "total_count": 1'
        "    },"
        '    "objects": ['
        "        {"
        '            "experiment_only_acls": false,'
        '            "identified_objects": ['
        '                "project",'
        '                "experiment",'
        '                "dataset",'
        '                "instrument",'
        '                "facility"'
        "            ],"
        '            "identifiers_enabled": false,'
        '            "profiled_objects": [],'
        '            "profiles_enabled": false,'
        '            "projects_enabled": true,'
        '            "resource_uri": "/api/v1/introspection/None/"'
        "        },"
        "        {"
        '            "experiment_only_acls": false,'
        '            "identified_objects": ['
        '                "project",'
        '                "experiment",'
        '                "dataset",'
        '                "instrument",'
        '                "facility"'
        "            ],"
        '            "identifiers_enabled": false,'
        '            "profiled_objects": [],'
        '            "profiles_enabled": false,'
        '            "projects_enabled": true,'
        '            "resource_uri": "/api/v1/introspection/None/"'
        "        }"
        "    ]"
        "}"
    )
    bad_dictionary = {
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
                    "project",
                    "experiment",
                    "dataset",
                    "instrument",
                    "facility",
                ],
                "identifiers_enabled": False,
                "profiled_objects": [],
                "profiles_enabled": False,
                "projects_enabled": True,
                "resource_uri": "/api/v1/introspection/None/",
            },
            {
                "experiment_only_acls": False,
                "identified_objects": [
                    "project",
                    "experiment",
                    "dataset",
                    "instrument",
                    "facility",
                ],
                "identifiers_enabled": False,
                "profiled_objects": [],
                "profiles_enabled": False,
                "projects_enabled": True,
                "resource_uri": "/api/v1/introspection/None/",
            },
        ],
    }
    mock_rest_factory_mytardis_api_request.return_value = mock_response
    with pytest.raises(ValueError) as error:
        test_overseer = Overseer(config_dict)  # pylint: disable=unused-variable

    assert caplog.record_tuples[0] == (
        "src.overseers.overseer",
        logging.ERROR,
        (
            "MyTardis introspection returned more than one object when called from "
            "Overseer.get_mytardis_set_up\n"
            f"Returned response was: {bad_dictionary}"
        ),
    )
    assert str(error.value) == (
        "MyTardis introspection returned more than one object when called from "
        "Overseer.get_mytardis_set_up"
    )
