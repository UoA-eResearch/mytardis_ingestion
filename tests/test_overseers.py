# pylint: disable=missing-function-docstring

"""Tests of the Overseer class and its functions"""
import logging

from src.overseers import Overseer

# import mock
# import pytest


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
