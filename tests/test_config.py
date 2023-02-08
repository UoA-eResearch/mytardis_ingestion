import logging
from urllib.parse import urljoin

import mock
import pytest
import responses
from requests.exceptions import HTTPError

from src.helpers.config import ConfigFromEnv

logger = logging.getLogger(__name__)
logger.propagate = True

# pylint: disable=missing-function-docstring
