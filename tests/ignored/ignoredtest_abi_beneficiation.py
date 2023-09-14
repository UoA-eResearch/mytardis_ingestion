# pylint: disable-all
# noqa
# nosec
# type: ignore

import logging
from typing import Literal
from unittest.mock import MagicMock, Mock

import pydantic
import pytest
from pytest import fixture

from src.beneficiations.beneficiation import Beneficiation
from src.extraction_output_manager.ingestibles import IngestibleDataclasses
from src.extraction_output_manager.output_manager import OutputManager
from src.profiles import profile_consts as pc
from src.profiles.profile_loader import ProfileLoader
from tests.fixtures.fixtures_abi_data import (
    get_abi_profile,
    get_bad_beneficiation_format,
    tmpdir_metadata_files,
)

logger = logging.getLogger(__name__)
logger.propagate = True


@pytest.mark.xfail
@pytest.mark.usefixtures("tmpdir_metadata_files", "get_abi_profile")
def test_json_parse(tmpdir_metadata_files, get_abi_profile):
    profile_loader: ProfileLoader = ProfileLoader(get_abi_profile)
    bnfc: Beneficiation = Beneficiation(profile_loader.load_custom_beneficiation())
    ingestible_dataclasses: IngestibleDataclasses = IngestibleDataclasses()

    ing_files = tmpdir_metadata_files[0]
    ing_dclasses = bnfc.beneficiate(
        beneficiation_data=ing_files, ingestible_dataclasses=ingestible_dataclasses
    )

    rawprojs = ing_dclasses.get_projects()
    rawexpts = ing_dclasses.get_experiments()
    rawdsets = ing_dclasses.get_datasets()
    rawdfiles = ing_dclasses.get_datafiles()

    assert len(rawprojs) == 1
    assert len(rawexpts) == 1
    assert len(rawdsets) == 2
    assert len(rawdfiles) == 1

    Beneficiation.clear()


@pytest.mark.xfail
@pytest.mark.usefixtures("tmpdir_metadata_files", "get_abi_profile")
def test_bad_json_parse(tmpdir_metadata_files, get_abi_profile):
    profile_loader = ProfileLoader(get_abi_profile)
    bnfc = Beneficiation(profile_loader.load_custom_beneficiation())
    ingestible_dataclasses: IngestibleDataclasses = IngestibleDataclasses()

    ing_files = tmpdir_metadata_files[1]
    with pytest.raises(pydantic.ValidationError):
        ing_dclasses = bnfc.beneficiate(
            beneficiation_data=ing_files, ingestible_dataclasses=ingestible_dataclasses
        )

    Beneficiation.clear()
