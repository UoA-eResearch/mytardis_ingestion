import logging
from typing import Literal
from unittest.mock import MagicMock, Mock

import pytest
from pytest import fixture

from src.beneficiations import beneficiation_consts as bc
from src.beneficiations.beneficiation import Beneficiation
from src.extraction_plant.extraction_plant import ExtractionPlant
from src.utils.ingestibles import IngestibleDataclasses
from src.miners.miner import Miner
from src.prospectors.prospector import Prospector
from tests.fixtures.fixtures_abi_data import get_abi_profile, tmpdir_metadata
from tests.fixtures.fixtures_abi_beneficiation import (spawn_bnfc)

logger = logging.getLogger(__name__)
logger.propagate = True


@pytest.mark.usefixtures("tmpdir_metadata", "get_abi_profile", "spawn_bnfc")
def test_extraction_plant(tmpdir_metadata, get_abi_profile, spawn_bnfc):
    bnfc = spawn_bnfc
    ingestible_dataclasses: IngestibleDataclasses = IngestibleDataclasses()
    prospector = Prospector(get_abi_profile)
    miner = Miner(get_abi_profile)
    ext_plant = ExtractionPlant(get_abi_profile, prospector, miner, bnfc)
    ingestible_dataclasses = ext_plant.run_extraction(
        tmpdir_metadata[0], ingestible_dataclasses
    )
    projs = ingestible_dataclasses.get_projects()
    expts = ingestible_dataclasses.get_experiments()
    dsets = ingestible_dataclasses.get_datasets()
    dfiles = ingestible_dataclasses.get_datafiles()

    proj = projs[0]
    expt = expts[0]

    assert proj.name == tmpdir_metadata[1]
    assert expt.title == tmpdir_metadata[2]
    assert len(dsets) == 2
    assert len(dfiles) == 3


@pytest.mark.usefixtures("tmpdir_metadata")
def test_extraction_plant_no_profile(tmpdir_metadata):
    bnfc = spawn_bnfc
    ingestible_dataclasses: IngestibleDataclasses = IngestibleDataclasses()
    prospector = Prospector(get_abi_profile)
    miner = Miner(get_abi_profile)
    ext_plant = ExtractionPlant(get_abi_profile, prospector, miner, bnfc)
    with pytest.raises(Exception):
        ext_plant.run_extraction(tmpdir_metadata[0], ingestible_dataclasses)
