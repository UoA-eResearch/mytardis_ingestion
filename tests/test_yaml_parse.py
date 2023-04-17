import pytest
import logging

from src.parsers.yaml_parser import YamlParser
from src.smelters.smelter import Smelter
from src.parsers.models import (  ### create new data model to match YAML file
    IngestionMetadata,
    RawProject,
   RawExperiment,
    RawDataset,
    RawDatafile,
)


logger = logging.getLogger(__name__)

def test_yaml_parse():
    yp = YamlParser()
    data_load = yp.parse_yaml_file("../test_renaming_exported_update.yaml")
    assert data_load[0].name == "p1"
    assert data_load[2].title == "e1"

def test_smelter():
    # check if smelter could do what he should do
    pass

def test_factory():
    # check if ingestion factory could do what he should do 
    pass
