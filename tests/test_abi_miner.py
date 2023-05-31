import logging
from typing import Literal
from unittest.mock import MagicMock, Mock

import pytest
from pytest import fixture

from src.miners.miner import Miner
from src.profiles import profile_consts as pc
from src.profiles.output_manager import OutputManager
from tests.fixtures.fixtures_abi_data import get_abi_profile, tmpdir_metadata

logger = logging.getLogger(__name__)
logger.propagate = True


@pytest.mark.usefixtures("tmpdir_metadata", "get_abi_profile")
def test_miner(tmpdir_metadata, get_abi_profile):
    miner = Miner(get_abi_profile)
    with pytest.raises(FileNotFoundError):
        out_man = miner.mine_directory(tmpdir_metadata[0])

    dir_to_ingore = tmpdir_metadata[3]
    file_to_ignore = tmpdir_metadata[4]
    out_man = OutputManager()
    out_man.add_dir_to_ignore(dir_to_ingore)
    out_man.add_file_to_ignore(file_to_ignore)

    new_out_man = miner.mine_directory(tmpdir_metadata[0], out_man=out_man)
    assert len(new_out_man.dirs_to_ignore) == 1
    assert len(new_out_man.files_to_ignore) == 1
    output_dict = new_out_man.output_dict
    issues = output_dict[pc.OUTPUT_ISSUES_KEY]
    ignores = output_dict[pc.OUTPUT_IGNORED_KEY]
    successes = output_dict[pc.OUTPUT_SUCCESS_KEY]
    assert len(issues) == 0
    assert len(ignores) == 0
    assert len(successes) == 7
