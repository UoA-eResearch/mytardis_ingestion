import logging
from typing import Literal
from unittest.mock import MagicMock, Mock

import pytest
from pytest import fixture

from src.beneficiations import beneficiation_consts as bc
from src.extraction_plant.extraction_plant import ExtractionPlant
from tests.fixtures.fixtures_abi_data import get_abi_profile, tmpdir_metadata

logger = logging.getLogger(__name__)
logger.propagate = True


@pytest.mark.usefixtures("tmpdir_metadata", "get_abi_profile")
def test_extraction_plant(tmpdir_metadata, get_abi_profile):
    ext_plant = ExtractionPlant(get_abi_profile)
    ingestible_dataclasses = ext_plant.run_extraction(
        tmpdir_metadata[0], bc.JSON_FORMAT
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
    ext_plant = ExtractionPlant()
    with pytest.raises(Exception):
        ext_plant.run_extraction(tmpdir_metadata[0], bc.JSON_FORMAT)
