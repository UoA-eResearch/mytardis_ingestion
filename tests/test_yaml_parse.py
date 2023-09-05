import logging

import pytest

from src.beneficiation.yaml_parser import YamlParser
from src.smelters.smelter import Smelter

logger = logging.getLogger(__name__)


def test_yaml_parse():
    yp = YamlParser()
    data_load = yp.parse_yaml_file("tests/fixtures/fixtures_example.yaml")
    assert data_load[0].name == "BIRU_MultipleLungCancer"
    assert data_load[2].title == "BIRU lungcancer1_NoTreatment"


### Could create these tests after the ingestion path is created - especially the .env file
def test_smelter():
    # check if smelter could do what he should do
    pass


def test_factory():
    # check if ingestion factory could do what he should do
    pass
