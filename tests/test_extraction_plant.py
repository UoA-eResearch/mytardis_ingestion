import logging
from typing import Literal
from unittest.mock import MagicMock, Mock

import pytest
from pytest import fixture

from src.beneficiations import beneficiation_consts as bc
from src.extraction_plant.extraction_plant import ExtractionPlant

logger = logging.getLogger(__name__)
logger.propagate = True


def test_extraction_plant():
    pass
    # TODO YJ: test_extraction_plant_using_IDW. This requires the IDW team to first finish their implementation so that the outputs can be simulated in fixtures,
    # and YJ to implement the interface between the IDW outputs to the beneficiation inputs.
