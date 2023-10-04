# pylint: disable=fixme
import logging
from pathlib import Path
from typing import Literal
from unittest.mock import MagicMock, Mock

import pytest
from pytest import fixture

from src.config.config import ConfigFromEnv
from src.extraction_output_manager.output_manager import OutputManager
from src.profiles import profile_consts as pc
from src.profiles.profile_loader import ProfileLoader
from src.prospectors.prospector import Prospector

logger = logging.getLogger(__name__)
logger.propagate = True
# TODO: Mock config config = ConfigFromEnv()


def test_prospector():
    pth = "tests/fixtures/fixtures_example.yaml"
    profile = str(Path("idw"))
    profile_loader = ProfileLoader(profile)

    prspctr = Prospector(profile_loader.load_custom_prospector())
    out_man = prspctr.prospect_directory(pth)

    assert len(out_man.dirs_to_ignore) == 0
    assert len(out_man.files_to_ignore) == 0
    output_dict = out_man.output_dict
    successes = output_dict[pc.OUTPUT_SUCCESS_KEY]
    assert len(successes) == 0

    Prospector.clear()
    ProfileLoader.clear()
