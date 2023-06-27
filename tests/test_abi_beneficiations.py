import logging
from typing import Literal
from unittest.mock import MagicMock, Mock

import pydantic
import pytest
from pytest import fixture

from src.beneficiations import beneficiation_consts as bc
from src.beneficiations.beneficiation import Beneficiation
from src.extraction_output_manager.output_manager import OutputManager
from src.profiles import profile_consts as pc
from src.utils.ingestibles import IngestibleDataclasses
from tests.fixtures.fixtures_abi_data import (
    get_bad_beneficiation_format,
    tmpdir_metadata_files,
)
from tests.fixtures.fixtures_abi_beneficiation import (spawn_bnfc)

logger = logging.getLogger(__name__)
logger.propagate = True

@pytest.mark.usefixtures("tmpdir_metadata_files", "spawn_bnfc")
def test_json_parse(tmpdir_metadata_files, spawn_bnfc):
    bnfc = spawn_bnfc
    ingestible_dataclasses: IngestibleDataclasses = IngestibleDataclasses()

    ing_files = tmpdir_metadata_files[0]
    ing_dclasses = bnfc.beneficiate(beneficiation_data = ing_files, 
                                    ingestible_dataclasses = ingestible_dataclasses)

    rawprojs = ing_dclasses.get_projects()
    rawexpts = ing_dclasses.get_experiments()
    rawdsets = ing_dclasses.get_datasets()
    rawdfiles = ing_dclasses.get_datafiles()

    assert len(rawprojs) == 1
    assert len(rawexpts) == 1
    assert len(rawdsets) == 2
    assert len(rawdfiles) == 1


@pytest.mark.usefixtures("tmpdir_metadata_files", "spawn_bnfc")
def test_bad_json_parse(tmpdir_metadata_files, spawn_bnfc):
    bnfc = spawn_bnfc
    ingestible_dataclasses: IngestibleDataclasses = IngestibleDataclasses()
    
    ing_files = tmpdir_metadata_files[1]
    with pytest.raises(pydantic.error_wrappers.ValidationError):
        ing_dclasses = bnfc.beneficiate(beneficiation_data = ing_files, 
                                    ingestible_dataclasses = ingestible_dataclasses)



# @pytest.mark.usefixtures("tmpdir_metadata_files", "get_bad_beneficiation_format", "spawn_bnfc")
# def test_bad_file_format(tmpdir_metadata_files, get_bad_beneficiation_format, spawn_bnfc):
#     bnfc = spawn_bnfc[0]
#     with pytest.raises(Exception):
#         ingestible_dataclasses = bnfc.beneficiate(
#             tmpdir_metadata_files[0],
#             tmpdir_metadata_files[1],
#             tmpdir_metadata_files[2],
#             tmpdir_metadata_files[4],
#             get_bad_beneficiation_format,
#         )
