import logging
from pathlib import Path

from src.profiles.idw.metadata_extraction import IDWMetadataExtractor

logger = logging.getLogger(__name__)
logger.propagate = True


def test_idw_extraction() -> None:
    """Tests that beneficiation for BIRU is successful and extracts metadata."""
    extractor = IDWMetadataExtractor()
    fpath = Path("tests/testdata/ingestion.yaml")
    ingestible_dataclasses = extractor.extract(fpath)
    df = ingestible_dataclasses.get_datafiles()[0]
    assert df.filename == "20221113_slide3-2_humanRWM_cd34_x20_0.12umpix_8.czi"
    assert df.metadata and df.metadata["Experimenter|UserName"] == "hsuz002"
    assert (
        df.metadata
        and df.metadata["Image|Pixels|Channel|Channel:0:0|PinholeSizeUnit"] == "um"
    )
    assert (
        df.metadata
        and df.metadata["Image|Pixels|Channel|Channel:0:1|PinholeSizeUnit"] == "um"
    )
    assert df.md5sum == "ba367447a14db59627850eed55a0d5f2"
