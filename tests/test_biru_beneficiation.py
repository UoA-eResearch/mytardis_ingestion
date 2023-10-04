import logging
import os
from pathlib import Path
from typing import Literal

import pytest
import yaml
from pytest import fixture

from src.extraction_output_manager.ingestibles import IngestibleDataclasses
from src.miners.utils import datafile_metadata_helpers
from src.profiles.idw.custom_beneficiation import CustomBeneficiation

logger = logging.getLogger(__name__)
logger.propagate = True


def test_beneficiate():
    a = CustomBeneficiation()
    fpath = "tests/fixtures/fixtures_example.yaml"
    ingestible_dataclasses = a.beneficiate(fpath, IngestibleDataclasses)
    df = ingestible_dataclasses.datafiles[0]
    """with open(fpath) as f:
        data = yaml.safe_load_all(f)
        for obj in data:
            print(obj)"""
    assert df.filename == "20221113_sheep_RWM_cd34_x20_0.12umpiz_01.czi"
    assert df.metadata["Experimenter|UserName"] == "hsuz002"


def test_beneficiate_replace_micrometer():
    a = CustomBeneficiation()
    fpath = "tests/fixtures/fixtures_example.yaml"
    ingestible_dataclasses = a.beneficiate(fpath, IngestibleDataclasses)
    df = ingestible_dataclasses.datafiles[0]
    assert df.filename == "20221113_sheep_RWM_cd34_x20_0.12umpiz_01.czi"
    assert df.metadata["Image|Pixels|Channel|Channel:0:0|PinholeSizeUnit"] == "um"
    assert df.metadata["Image|Pixels|Channel|Channel:0:1|PinholeSizeUnit"] == "um"
    assert df.md5sum == "ff19746f8738686e51e2f7eb91dbed00"
