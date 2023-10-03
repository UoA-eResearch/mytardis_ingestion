import logging
import os
from pathlib import Path
from typing import Literal
import yaml

import pytest
from pytest import fixture

from src.profiles.idw.custom_beneficiation import CustomBeneficiation
from src.extraction_output_manager.ingestibles import IngestibleDataclasses


logger = logging.getLogger(__name__)
logger.propagate = True

def test_beneficiate():
    a = CustomBeneficiation()
    fpath = "tests/fixtures/fixtures_example.yaml"
    ingestible_dataclasses = a.beneficiate(fpath, IngestibleDataclasses)
    df = ingestible_dataclasses.datafiles[0]
    '''with open(fpath) as f:
        data = yaml.safe_load_all(f)
        for obj in data:
            print(obj)'''
    assert df.filename == "20221113_sheep_RWM_cd34_x20_0.12umpiz_01.czi"
    assert df.metadata['Experimenter|UserName']== 'hsuz002'