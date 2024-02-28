import logging
from pathlib import Path

from src.extraction.manifest import IngestionManifest
from src.profiles.idw.custom_beneficiation import CustomBeneficiation

logger = logging.getLogger(__name__)
logger.propagate = True


def test_beneficiate():
    """Tests that beneficiation for BIRU is successful and extracts metadata."""
    a = CustomBeneficiation()
    fpath = "tests/testdata/"
    ingestible_dataclasses = a.beneficiate(Path(fpath), IngestionManifest())
    df = ingestible_dataclasses.get_datafiles()[0]
    assert df.filename == "20221113_slide3-2_humanRWM_cd34_x20_0.12umpix_3.czi"
    assert df.metadata and df.metadata["Experimenter|UserName"] == "hsuz002"
    assert (
        df.metadata
        and df.metadata["Image|Pixels|Channel|Channel:0:0|PinholeSizeUnit"] == "um"
    )
    assert (
        df.metadata
        and df.metadata["Image|Pixels|Channel|Channel:0:1|PinholeSizeUnit"] == "um"
    )
    assert df.md5sum == "66dae4208956c7b0081bd31acae35506"
