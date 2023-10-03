import logging
from typing import Literal
from unittest.mock import MagicMock, Mock

import pydantic
import pytest
from pytest import fixture

from src.beneficiations import beneficiation_consts as bc
from src.beneficiations.beneficiation import Beneficiation
from src.extraction_output_manager.ingestibles import IngestibleDataclasses
from src.extraction_output_manager.output_manager import OutputManager
from src.profiles import profile_consts as pc
from tests.fixtures.fixtures_abi_data import (
    get_bad_beneficiation_format,
    tmpdir_metadata_files,
)

logger = logging.getLogger(__name__)
logger.propagate = True


@pytest.mark.xfail
@pytest.mark.usefixtures("tmpdir_metadata_files")
def test_json_parse(tmpdir_metadata_files):
    bnfc = Beneficiation()
    ingestible_dataclasses = bnfc.beneficiate(
        tmpdir_metadata_files[0],
        tmpdir_metadata_files[1],
        tmpdir_metadata_files[2],
        tmpdir_metadata_files[3],
        bc.JSON_FORMAT,
    )

    rawprojs = ingestible_dataclasses.get_projects()
    rawexpts = ingestible_dataclasses.get_experiments()
    rawdsets = ingestible_dataclasses.get_datasets()
    rawdfiles = ingestible_dataclasses.get_datafiles()

    assert len(rawprojs) == 1
    assert len(rawexpts) == 1
    assert len(rawdsets) == 2
    assert len(rawdfiles) == 1


@pytest.mark.xfail
@pytest.mark.usefixtures("tmpdir_metadata_files")
def test_bad_json_parse(tmpdir_metadata_files):
    bnfc = Beneficiation()
    with pytest.raises(pydantic.error_wrappers.ValidationError):
        ingestible_dataclasses = bnfc.beneficiate(
            tmpdir_metadata_files[0],
            tmpdir_metadata_files[1],
            tmpdir_metadata_files[2],
            tmpdir_metadata_files[4],
            bc.JSON_FORMAT,
        )


@pytest.mark.xfail
@pytest.mark.usefixtures("tmpdir_metadata_files", "get_bad_beneficiation_format")
def test_bad_file_format(tmpdir_metadata_files, get_bad_beneficiation_format):
    bnfc = Beneficiation()
    with pytest.raises(Exception):
        ingestible_dataclasses = bnfc.beneficiate(
            tmpdir_metadata_files[0],
            tmpdir_metadata_files[1],
            tmpdir_metadata_files[2],
            tmpdir_metadata_files[4],
            get_bad_beneficiation_format,
        )
