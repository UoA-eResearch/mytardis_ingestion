# pylint: disable=missing-docstring
# nosec assert_used
import logging
import tempfile

import yaml
from pytest import fixture

from src.extraction.manifest import IngestionManifest
from src.profiles.idw.custom_beneficiation import CustomBeneficiation

logger = logging.getLogger(__name__)
logger.propagate = True


def test_beneficiate() -> None:
    with tempfile.NamedTemporaryFile(
        mode="w", delete=False, suffix=".yaml"
    ) as tmp_yaml_file:
        yaml_data = {"key1": "value1", "key2": "value2"}
        yaml.dump(yaml_data, tmp_yaml_file, default_flow_style=False)
    a = CustomBeneficiation()
    fpath = "tests/testdata/test_ingestion.yaml"
    ingestible_dataclasses = a.beneficiate(fpath, IngestionManifest)
    df = ingestible_dataclasses.get_datafiles()[0]
    assert df.filename == "20221113_slide3-2_humanRWM_cd34_x20_0.12umpix_8.czi"
    assert df.metadata["Experimenter|UserName"] == "hsuz002"


def test_beneficiate_replace_micrometer() -> None:
    a = CustomBeneficiation()
    fpath = "tests/testdata/test_ingestion.yaml"
    ingestible_dataclasses = a.beneficiate(fpath, IngestionManifest)
    df = ingestible_dataclasses.get_datafiles()[0]
    assert df.filename == "20221113_slide3-2_humanRWM_cd34_x20_0.12umpix_8.czi"
    assert df.metadata["Image|Pixels|Channel|Channel:0:0|PinholeSizeUnit"] == "um"
    assert df.metadata["Image|Pixels|Channel|Channel:0:1|PinholeSizeUnit"] == "um"
    assert df.md5sum == "ba367447a14db59627850eed55a0d5f2"
