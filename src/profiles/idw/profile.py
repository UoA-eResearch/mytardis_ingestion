"""
Ingestion profile for ingestions using the Instrument Data Wizard (IDW)
"""

from pathlib import Path
from typing import Optional

from src.beneficiations.beneficiation import Beneficiation
from src.extraction.extraction_plant import ExtractionPlant
from src.extraction.metadata_extractor import IMetadataExtractor
from src.profiles.idw.cleanup import idw_cleanup
from src.profiles.idw.custom_beneficiation import CustomBeneficiation  # type: ignore
from src.profiles.profile_base import IProfile


class IDWProfile(IProfile):
    """Profile defining the ingestion behaviour for metadata from the Instrument Data Wizard"""

    def __init__(self) -> None:
        pass

    @property
    def name(self) -> str:
        return "idw"

    def get_extractor(self) -> IMetadataExtractor:
        return ExtractionPlant(
            prospector=None,
            miner=None,
            beneficiation=Beneficiation(beneficiation=CustomBeneficiation()),
        )

    def cleanup(self, source_data_path: Path) -> None:
        return idw_cleanup(source_data_path)


def get_profile(version: Optional[str]) -> IProfile:
    """Entry point for the profile - returns the profile corresponding to the requested version"""
    match version:
        case "v1" | None:
            return IDWProfile()
        case _:
            raise ValueError(f"Unknown version '{version}' for 'IDW' profile")
