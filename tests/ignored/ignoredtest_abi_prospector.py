import logging
from typing import Literal
from unittest.mock import MagicMock, Mock

import pytest
from pytest import fixture

from src.extraction_output_manager.output_manager import OutputManager
from src.profiles import profile_consts as pc
from src.profiles.profile_loader import ProfileLoader
from src.prospectors.prospector import Prospector
from tests.fixtures.fixtures_abi_data import get_abi_profile, tmpdir_metadata

logger = logging.getLogger(__name__)
logger.propagate = True


@pytest.mark.usefixtures("tmpdir_metadata", "get_abi_profile")
def test_prospector(tmpdir_metadata, get_abi_profile):
    profile_loader = ProfileLoader(get_abi_profile)

    prspctr = Prospector(profile_loader.load_custom_prospector())
    out_man = prspctr.prospect_directory(tmpdir_metadata[0])

    assert len(out_man.dirs_to_ignore) == 1
    assert len(out_man.files_to_ignore) == 1
    output_dict = out_man.output_dict
    successes = output_dict[pc.OUTPUT_SUCCESS_KEY]
    assert len(successes) == 4

    Prospector.clear()
    ProfileLoader.clear()
