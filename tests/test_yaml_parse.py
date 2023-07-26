import logging

import pytest
import yaml

from src.extraction_output_manager.ingestibles import IngestibleDataclasses
from src.profiles.idw.custom_beneficiation import CustomBeneficiation

# from src.smelters.smelter import Smelter

logger = logging.getLogger(__name__)


def test_beneficiation():
    # check if beneficiation could do what he should do
    yp = CustomBeneficiation()
    ing_dclass = yp.beneficiate(
        "tests/fixtures/fixtures_example.yaml", IngestibleDataclasses
    )
    print(ing_dclass.projects)
    print(ing_dclass.experiments)
    print(ing_dclass.datasets)
    print(ing_dclass.datafiles)
    # ingestion_output = yp.beneficiate(data_load, IngestibleDataclasses)


### Could create these tests after the ingestion path is created - especially the .env file
def test_smelter():
    # check if smelter could do what he should do
    pass


def test_factory():
    # check if ingestion factory could do what he should do
    pass
