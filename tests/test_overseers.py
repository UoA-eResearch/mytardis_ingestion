# pylint: disable=missing-function-docstring

"""Tests of the Overseer class and its functions"""
import logging

import responses

from src.overseers import Overseer

# from requests import Response


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


@responses.activate
def test_get_uris():
    responses.add(
        responses.GET,
        "https://test.mytardis.nectar.auckland.ac.nz/api/v1/institution",
        json=(
            "{"
            '"meta": {'
            '	"limit": 20,'
            '	"next": null,'
            '	"offset": 0,'
            '	"previous": null,'
            '	"total_count": 1'
            "},"
            '"objects": [{'
            '	"address": "words",'
            '	"aliases": 1,'
            '	"alternate_ids": ['
            '		"fruit",'
            '		"apples"'
            "	],"
            '	"country": "NZ",'
            '	"name": "University of Auckland",'
            '	"persistent_id": "BANANA",'
            '	"resource_uri": "/api/v1/institution/1/"'
            "}]"
            "}"
        ),
        status=200,
    )
    test_overseer = Overseer(config_dict)
    assert test_overseer.get_uris("institution", "pids", "Uni ROR") == [
        "/api/v1/institution/1/"
    ]
