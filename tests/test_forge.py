# pylint: disable=missing-function-docstring

"""Tests of the Forge functions"""

import logging

import mock
import responses

from src.forges import Forge

config_dict = {
    "username": "Test_User",
    "api_key": "Test_API_Key",
    "hostname": "https://test.mytardis.nectar.auckland.ac.nz",
    "verify_certificate": True,
    "proxy_http": "http://myproxy.com",
    "proxy_https": "http://myproxy.com",
}

logger = logging.getLogger(__name__)
logger.propagate = True
